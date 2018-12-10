import time
import os
import os.path
import logging
import socket
import contextlib
import argparse
import asyncio
import concurrent.futures
from typing import Optional, Callable       # noqa: F401

import h5py
import numpy as np

import aiokatcp
from aiokatcp import FailReply, Sensor

import katsdpservices
import katsdptelstate

from ._bf_ingest import Session, SessionConfig, ReceiverCounters
from . import utils, file_writer
from .utils import Range
import katsdpbfingest


_logger = logging.getLogger(__name__)


def _warn_if_positive(value: float) -> aiokatcp.Sensor.Status:
    """Status function for sensors that count problems"""
    return Sensor.Status.WARN if value > 0 else Sensor.Status.NOMINAL


def _config_from_telstate(telstate: katsdptelstate.TelescopeState,
                          config: SessionConfig,
                          attr_name: str, telstate_name: str = None) -> None:
    """Populate a SessionConfig from telstate entries.

    Parameters
    ----------
    telstate
        Telescope state with views for the CBF stream
    config
        Configuration object to populate
    attr_name
        Attribute name to set in `config`
    telstate_name
        Name to look up in `telstate` (defaults to `attr_name`)
    """
    if telstate_name is None:
        telstate_name = attr_name
    value = telstate[telstate_name]
    _logger.info('Setting %s to %s from telstate', attr_name, value)
    setattr(config, attr_name, value)


def _create_session_config(args: argparse.Namespace) -> SessionConfig:
    """Creates a SessionConfig object for a :class:`CaptureServer`.

    Note that this function makes blocking calls to telstate. The returned
    config has no filename.

    Parameters
    ----------
    args
        Command-line arguments. See :class:`CaptureServer`.
    """
    config = SessionConfig('')  # Real filename supplied later
    config.max_packet = args.max_packet
    config.buffer_size = args.buffer_size
    if args.interface is not None:
        config.interface_address = katsdpservices.get_interface_address(args.interface)
    config.ibv = args.ibv
    if args.affinity:
        config.disk_affinity = args.affinity[0]
        config.network_affinity = args.affinity[1]
    if args.direct_io:
        config.direct = True

    # Load external config from telstate
    telstate = utils.cbf_telstate_view(args.telstate, args.stream_name)
    _config_from_telstate(telstate, config, 'channels', 'n_chans')
    _config_from_telstate(telstate, config, 'channels_per_heap', 'n_chans_per_substream')
    for name in ['ticks_between_spectra', 'spectra_per_heap', 'sync_time',
                 'bandwidth', 'center_freq', 'scale_factor_timestamp']:
        _config_from_telstate(telstate, config, name)

    # Check that the requested channel range is valid.
    all_channels = Range(0, config.channels)
    if args.channels is None:
        args.channels = all_channels
    channels_per_endpoint = config.channels // len(args.cbf_spead)
    if not args.channels.isaligned(channels_per_endpoint):
        raise ValueError(
            '--channels is not aligned to multiples of {}'.format(channels_per_endpoint))
    if not args.channels.issubset(all_channels):
        raise ValueError(
            '--channels does not fit inside range {}'.format(all_channels))

    # Update for selected channel range
    channel_shift = (args.channels.start + args.channels.stop - config.channels) / 2
    config.center_freq += channel_shift * config.bandwidth / config.channels
    config.bandwidth = config.bandwidth * len(args.channels) / config.channels
    config.channels = len(args.channels)
    config.channel_offset = args.channels.start

    endpoint_range = np.s_[args.channels.start // channels_per_endpoint:
                           args.channels.stop // channels_per_endpoint]
    for endpoint in args.cbf_spead[endpoint_range]:
        config.add_endpoint(socket.gethostbyname(endpoint.host), endpoint.port)
    if args.stats is not None:
        config.set_stats_endpoint(args.stats.host, args.stats.port)
        config.stats_int_time = args.stats_int_time
        if args.stats_interface is not None:
            config.stats_interface_address = \
                katsdpservices.get_interface_address(args.stats_interface)

    return config


class _CaptureSession:
    """Object encapsulating a co-routine that runs for a single capture session
    (from ``capture-init`` to end of stream or ``capture-done``).

    Parameters
    ----------
    config
        Configuration generated by :meth:`create_session_config`
    telstate
        Telescope state (optional)
    stream_name
        Name of the beamformer stream being captured
    update_counters
        Called once a second with progress counters
    loop
        IO Loop for the coroutine

    Attributes
    ----------
    filename : :class:`str` or ``None``
        Filename of the HDF5 file written
    _telstate : :class:`katsdptelstate.TelescopeState`
        Telescope state interface, if any
    _loop : :class:`asyncio.AbstractEventLoop`
        Event loop passed to the constructor
    _session : :class:`katsdpbfingest._bf_ingest.Session`
        C++-driven capture session
    _run_future : :class:`asyncio.Task`
        Task for the coroutine that waits for the C++ code and finalises
    """
    def __init__(self, config: SessionConfig, telstate: katsdptelstate.TelescopeState,
                 stream_name: str, update_counters: Callable[[ReceiverCounters], None],
                 loop: asyncio.AbstractEventLoop) -> None:
        self._start_time = time.time()
        self._loop = loop
        self._telstate = telstate
        self.filename = config.filename
        self.stream_name = stream_name
        self.update_counters = update_counters
        self._config = config
        self._session = Session(config)
        self._run_future = loop.create_task(self._run())

    def _write_metadata(self) -> None:
        telstate = self._telstate
        view = utils.cbf_telstate_view(telstate, self.stream_name)
        try:
            sync_time = view['sync_time']
            scale_factor_timestamp = view['scale_factor_timestamp']
            first_timestamp = sync_time + self._session.first_timestamp / scale_factor_timestamp
        except KeyError:
            _logger.warn('Failed to get timestamp conversion items, so skipping metadata')
            return
        # self._start_time should always be earlier, except when a clock is wrong.
        start_time = min(first_timestamp, self._start_time)
        h5file = h5py.File(self.filename, 'r+')
        with contextlib.closing(h5file):
            file_writer.set_telescope_state(h5file, telstate, start_timestamp=start_time)
            if self.stream_name is not None:
                data_group = h5file['/Data']
                data_group.attrs['stream_name'] = self.stream_name
                data_group.attrs['channel_offset'] = self._config.channel_offset

    async def _run(self) -> None:
        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            try:
                self.update_counters(self._session.counters)
                # Just passing self._session.join causes an exception in Python
                # 3.5 because iscoroutinefunction doesn't work on functions
                # defined by extensions. Hence the lambda.
                join_future = self._loop.run_in_executor(pool, lambda: self._session.join())
                # Update the sensors once per second until termination
                while not (await asyncio.wait([join_future], loop=self._loop, timeout=1.0))[0]:
                    self.update_counters(self._session.counters)
                await join_future   # To re-raise any exception
                counters = self._session.counters
                self.update_counters(counters)
                if counters.heaps > 0 and self.filename is not None:
                    # Write the metadata to file
                    self._write_metadata()
                _logger.info('Capture complete, %d heaps, of which %d dropped',
                             counters.total_heaps,
                             counters.total_heaps - counters.heaps)
            except Exception:
                _logger.error("Capture threw exception", exc_info=True)

    async def stop(self) -> None:
        """Shut down the stream and wait for the session to end. This
        is a coroutine.
        """
        self._session.stop_stream()
        await self._run_future


class CaptureServer:
    """Beamformer capture. This contains all the core functionality of the
    katcp device server, without depending on katcp. It is split like this
    to facilitate unit testing.

    Parameters
    ----------
    args
        Command-line arguments. The following arguments are must be present, although
        some of them can be ``None``. Refer to the script for documentation of
        these options.

        - cbf_spead
        - file_base
        - buffer
        - affinity
        - telstate
        - stream_name
        - stats
        - stats_int_time
        - stats_interface

    loop
        IO Loop for running coroutines

    Attributes
    ----------
    capturing : :class:`bool`
        Whether a capture session is in progress. Note that a session is
        considered to be in progress until explicitly stopped with
        :class:`stop_capture`, even if the stream has terminated.
    _args : :class:`argparse.Namespace`
        Command-line arguments passed to constructor
    _loop : :class:`asyncio.AbstractEventLoop`
        IO Loop passed to constructor
    _capture : :class:`_CaptureSession`
        Current capture session, or ``None`` if not capturing
    _config : :class:`katsdpbfingest.bf_ingest.SessionConfig`
        Configuration, with the filename to be filled in on capture-init
    """
    def __init__(self, args: argparse.Namespace, loop: asyncio.AbstractEventLoop) -> None:
        self._args = args
        self._loop = loop
        self._capture = None      # type: Optional[_CaptureSession]
        self._config = _create_session_config(args)

    @property
    def capturing(self):
        return self._capture is not None

    async def start_capture(self, capture_block_id: str) -> str:
        """Start capture, if not already in progress.

        This is a co-routine.
        """
        if self._capture is None:
            if self._args.file_base is not None:
                basename = '{}_{}.h5'.format(capture_block_id, self._args.stream_name)
                self._config.filename = os.path.join(self._args.file_base, basename)
            else:
                self._config.filename = None
            self._capture = _CaptureSession(
                self._config, self._args.telstate, self._args.stream_name,
                self.update_counters, self._loop)
        return self._capture.filename

    async def stop_capture(self) -> None:
        """Stop capture, if currently running. This is a co-routine."""
        if self._capture is not None:
            capture = self._capture
            await capture.stop()
            # Protect against a concurrent stop and start changing to a new
            # capture.
            if self._capture is capture:
                self._capture = None

    def update_counters(self, counters: ReceiverCounters) -> None:
        pass   # Implemented by subclass


class KatcpCaptureServer(CaptureServer, aiokatcp.DeviceServer):
    """katcp device server for beamformer capture.

    Parameters
    ----------
    args : :class:`argparse.Namespace`
        Command-line arguments (see :class:`CaptureServer`).
        The following additional arguments are required:

        host
          Hostname to bind to ('' for none)
        port
          Port number to bind to
    loop : :class:`asyncio.AbstractEventLoop`
        IO Loop for running coroutines
    """

    VERSION = 'bf-ingest-1.0'
    BUILD_STATE = 'katsdpbfingest-' + katsdpbfingest.__version__

    def __init__(self, args: argparse.Namespace, loop: asyncio.AbstractEventLoop) -> None:
        CaptureServer.__init__(self, args, loop)
        aiokatcp.DeviceServer.__init__(self, args.host, args.port, loop=loop)
        sensors = [
            Sensor(int, "input-heaps-total",
                   "Number of payload heaps received from CBF in this session "
                   "(prometheus: counter)",
                   initial_status=Sensor.Status.NOMINAL),
            Sensor(int, "input-bytes-total",
                   "Number of payload bytes received from CBF in this session "
                   "(prometheus: counter)",
                   initial_status=Sensor.Status.NOMINAL),
            Sensor(int, "input-missing-heaps-total",
                   "Number of heaps we expected but never saw "
                   "(prometheus: counter)",
                   initial_status=Sensor.Status.NOMINAL,
                   status_func=_warn_if_positive),
            Sensor(int, "input-too-old-heaps-total",
                   "Number of heaps rejected because they arrived too late "
                   "(prometheus: counter)",
                   initial_status=Sensor.Status.NOMINAL,
                   status_func=_warn_if_positive),
            Sensor(int, "input-incomplete-heaps-total",
                   "Number of heaps rejected due to missing packets "
                   "(prometheus: counter)",
                   initial_status=Sensor.Status.NOMINAL,
                   status_func=_warn_if_positive),
            Sensor(int, "input-bad-metadata-heaps-total",
                   "Number of heaps rejected due to bad timestamp or channel "
                   "(prometheus: counter)",
                   initial_status=Sensor.Status.NOMINAL,
                   status_func=_warn_if_positive),
            Sensor(int, "input-packets-total",
                   "Total number of packets received (prometheus: counter)",
                   initial_status=Sensor.Status.NOMINAL),
            Sensor(int, "input-batches-total",
                   "Number of batches of packets processed (prometheus: counter)",
                   initial_status=Sensor.Status.NOMINAL),
            Sensor(int, "input-max-batch",
                   "Maximum number of packets processed in a batch (prometheus: gauge)",
                   initial_status=Sensor.Status.NOMINAL)
        ]
        for sensor in sensors:
            self.sensors.add(sensor)

    def update_counters(self, counters: ReceiverCounters) -> None:
        timestamp = time.time()
        for name in ['bytes', 'packets', 'batches',
                     'heaps', 'too-old-heaps', 'incomplete-heaps', 'bad-metadata-heaps']:
            sensor = self.sensors['input-{}-total'.format(name)]
            value = getattr(counters, name.replace('-', '_'))
            sensor.set_value(value, timestamp=timestamp)
        for name in ['max-batch']:
            sensor = self.sensors['input-{}'.format(name)]
            value = getattr(counters, name.replace('-', '_'))
            sensor.set_value(value, timestamp=timestamp)
        self.sensors['input-missing-heaps-total'].set_value(
            counters.total_heaps - counters.heaps, timestamp=timestamp)

    async def request_capture_init(self, ctx, capture_block_id: str) -> None:
        """Start capture to file."""
        if self.capturing:
            raise FailReply('already capturing')
        if self._args.file_base is not None:
            stat = os.statvfs(self._args.file_base)
            if stat.f_bavail / stat.f_blocks < 0.05:
                raise FailReply('less than 5% disk space free on {}'.format(
                    os.path.abspath(self._args.file_base)))
        await self.start_capture(capture_block_id)

    async def request_capture_done(self, ctx) -> None:
        """Stop a capture that is in progress."""
        if not self.capturing:
            raise FailReply('not capturing')
        await self.stop_capture()

    async def stop(self, cancel: bool = True) -> None:
        await self.stop_capture()
        await aiokatcp.DeviceServer.stop(self, cancel)

    stop.__doc__ = aiokatcp.DeviceServer.stop.__doc__


__all__ = ['CaptureServer', 'KatcpCaptureServer']
