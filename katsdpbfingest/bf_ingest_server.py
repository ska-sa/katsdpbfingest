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
import spead2

import katsdpservices
import katsdptelstate
from katsdptelstate.endpoint import endpoints_to_str

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


def create_session_config(args: argparse.Namespace) -> SessionConfig:
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

    # Set up batching to get 32MB per slice
    config.heaps_per_slice_time = max(1, 2**25 // (config.channels * config.spectra_per_heap * 2))
    # 256MB of buffer
    config.ring_slots = 8

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
    endpoints = args.cbf_spead[endpoint_range]
    for endpoint in endpoints:
        config.add_endpoint(socket.gethostbyname(endpoint.host), endpoint.port)
    config.endpoints_str = endpoints_to_str(endpoints)
    if args.stats is not None:
        config.set_stats_endpoint(args.stats.host, args.stats.port)
        config.stats_int_time = args.stats_int_time
        if args.stats_interface is not None:
            config.stats_interface_address = \
                katsdpservices.get_interface_address(args.stats_interface)

    return config


def session_factory(config: SessionConfig) -> Session:
    """Thin wrapper around :class:`_bf_ingest.Session` constructor to ease mocking."""
    return Session(config)


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
    write_telstate
        Whether to write TelescopeState into the output HDF5 file
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
    _write_telstate : :class:`bool`
        Whether to write TelescopeState into the output HDF5 file
    _loop : :class:`asyncio.AbstractEventLoop`
        Event loop passed to the constructor
    _session : :class:`katsdpbfingest._bf_ingest.Session`
        C++-driven capture session
    _run_future : :class:`asyncio.Task`
        Task for the coroutine that waits for the C++ code and finalises
    """
    def __init__(self, config: SessionConfig, telstate: katsdptelstate.TelescopeState,
                 stream_name: str, write_telstate: bool,
                 update_counters: Callable[[ReceiverCounters], None],
                 loop: asyncio.AbstractEventLoop) -> None:
        self._start_time = time.time()
        self._loop = loop
        self._telstate = telstate
        self.filename = config.filename
        self.stream_name = stream_name
        self._write_telstate = write_telstate
        self.update_counters = update_counters
        self._config = config
        self._session = session_factory(config)
        self._run_future = loop.create_task(self._run())

    def _write_metadata(self) -> None:
        if self._write_telstate:
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
            if self._write_telstate:
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
                while not (await asyncio.wait([join_future], timeout=1.0))[0]:
                    self.update_counters(self._session.counters)
                await join_future   # To re-raise any exception
                counters = self._session.counters
                self.update_counters(counters)
                if counters["katsdpbfingest.data_heaps"] > 0 and self.filename is not None:
                    # Write the metadata to file
                    self._write_metadata()
                _logger.info(
                    'Capture complete, %d heaps, of which %d dropped',
                    counters["katsdpbfingest.total_heaps"],
                    counters["katsdpbfingest.total_heaps"] - counters["katsdpbfingest.data_heaps"])
            except Exception:
                _logger.error("Capture threw exception", exc_info=True)

    async def stop(self, force: bool = True) -> None:
        """Shut down the stream and wait for the session to end. This
        is a coroutine.

        If `force` is False, it will wait until the stream stops on its
        own in response to a stop heap.
        """
        if force:
            self._session.stop_stream()
        await self._run_future

    @property
    def counters(self) -> ReceiverCounters:
        return self._session.counters


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
        - write_telstate
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
        self._config = create_session_config(args)

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
                self._args.write_telstate, self.update_counters, self._loop)
        return self._capture.filename

    async def stop_capture(self, force: bool = True) -> None:
        """Stop capture, if currently running. This is a co-routine."""
        if self._capture is not None:
            capture = self._capture
            await capture.stop(force)
            # Protect against a concurrent stop and start changing to a new
            # capture.
            if self._capture is capture:
                self._capture = None

    def update_counters(self, counters: ReceiverCounters) -> None:
        pass   # Implemented by subclass

    @property
    def counters(self) -> ReceiverCounters:
        if self._capture is not None:
            return self._capture.counters
        else:
            return ReceiverCounters()


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
            Sensor(int, "input-metadata-heaps-total",
                   "Number of heaps that do not contain data "
                   "(prometheus: counter)",
                   initial_status=Sensor.Status.NOMINAL),
            Sensor(int, "input-bad-timestamp-heaps-total",
                   "Number of heaps rejected due to bad timestamp "
                   "(prometheus: counter)",
                   initial_status=Sensor.Status.NOMINAL,
                   status_func=_warn_if_positive),
            Sensor(int, "input-bad-channel-heaps-total",
                   "Number of heaps rejected due to bad channel offset "
                   "(prometheus: counter)",
                   initial_status=Sensor.Status.NOMINAL,
                   status_func=_warn_if_positive),
            Sensor(int, "input-bad-length-heaps-total",
                   "Number of heaps rejected due to bad payload length "
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
        # Map sensor name to counter name
        sensors = {
            'input-bytes-total': 'katsdpbfingest.bytes',
            'input-packets-total': 'packets',
            'input-batches-total': 'batches',
            'input-heaps-total': 'katsdpbfingest.data_heaps',
            'input-too-old-heaps-total': 'too_old_heaps',
            'input-incomplete-heaps-total': 'incomplete_heaps_evicted',
            'input-metadata-heaps-total': 'katsdpbfingest.metadata_heaps',
            'input-bad-timestamp-heaps-total': 'katsdpbfingest.bad_timestamp_heaps',
            'input-bad-channel-heaps-total': 'katsdpbfingest.bad_channel_heaps',
            'input-bad-length-heaps-total': 'katsdpbfingest.bad_length_heaps',
            'input-max-batch': 'max_batch'
        }
        for sensor_name, counter_name in sensors.items():
            sensor = self.sensors[sensor_name]
            value = counters[counter_name]
            sensor.set_value(value, timestamp=timestamp)
        self.sensors['input-missing-heaps-total'].set_value(
            counters["katsdpbfingest.total_heaps"] - counters["katsdpbfingest.data_heaps"],
            timestamp=timestamp
        )

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


def parse_args(args=None, namespace=None):
    """Parse command-line arguments.

    Any arguments are forwarded to :meth:`katsdpservices.ArgumentParser.parse_args`.
    """
    defaults = SessionConfig('')
    parser = katsdpservices.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--cbf-spead', type=katsdptelstate.endpoint.endpoint_list_parser(7148),
        default=':7148', metavar='ENDPOINTS',
        help=('endpoints to listen for CBF SPEAD stream (including multicast IPs). '
              '[<ip>[+<count>]][:port].'))
    parser.add_argument(
        '--stream-name', type=str, metavar='NAME',
        help='Stream name for metadata in telstate')
    parser.add_argument(
        '--channels', type=Range.parse, metavar='A:B',
        help='Output channels')
    parser.add_argument(
        '--log-level', '-l', type=str, metavar='LEVEL', default=None,
        help='log level')
    parser.add_argument(
        '--file-base', type=str, metavar='DIR',
        help='write HDF5 files in this directory if given')
    parser.add_argument(
        '--affinity', type=spead2.parse_range_list, metavar='CPU,CPU',
        help='List of CPUs to which to bind threads')
    parser.add_argument(
        '--interface', type=str,
        help='Network interface for multicast subscription')
    parser.add_argument(
        '--direct-io', action='store_true',
        help='Use Direct I/O VFD for writing the file')
    parser.add_argument(
        '--ibv', action='store_true',
        help='Use libibverbs when possible')
    parser.add_argument(
        '--buffer-size', type=int, metavar='BYTES', default=defaults.buffer_size,
        help='Network buffer size [%(default)s]')
    parser.add_argument(
        '--max-packet', type=int, metavar='BYTES', default=defaults.max_packet,
        help='Maximum packet size (UDP payload) [%(default)s]')
    parser.add_argument(
        '--stats', type=katsdptelstate.endpoint.endpoint_parser(7149), metavar='ENDPOINT',
        help='Send statistics to a signal display server at this address')
    parser.add_argument(
        '--stats-int-time', type=float, default=1.0, metavar='SECONDS',
        help='Interval between sending statistics to the signal displays')
    parser.add_argument(
        '--stats-interface', type=str,
        help='Network interface for signal display stream')
    parser.add_argument(
        '--write-telstate', action='store_true',
        help='Write TelescopeState into the HDF5 file')
    parser.add_aiomonitor_arguments()
    parser.add_argument('--port', '-p', type=int, default=2050, help='katcp host port')
    parser.add_argument('--host', '-a', type=str, default='', help='katcp host address')
    args = parser.parse_args(args, namespace)
    if args.affinity and len(args.affinity) < 2:
        parser.error('At least 2 CPUs must be specified for --affinity')
    if args.telstate is None:
        parser.error('--telstate is required')
    if args.stream_name is None:
        parser.error('--stream-name is required')
    return args


__all__ = ['CaptureServer', 'KatcpCaptureServer', 'parse_args']
