from __future__ import print_function, division, absolute_import
import spead2
import spead2.recv
import spead2.recv.trollius
import katcp
from katcp.kattypes import request, return_reply
from katsdpfilewriter import telescope_model, ar1_model, file_writer
from ._bf_ingest_session import Session, SessionConfig
import h5py
import numpy as np
import time
import os
import os.path
import trollius
from trollius import From, Return
import tornado
import logging
import katsdpingest
import katsdpservices
import psutil
import ipaddress
import socket
import contextlib
import concurrent.futures


_logger = logging.getLogger(__name__)


def _config_from_telstate(args, config, attr_name, telstate_name, use_stream_name=True):
    if use_stream_name:
        normalised = args.stream_name.replace('.', '_').replace('-', '_')
        telstate_name = '{}_{}'.format(normalised, telstate_name)
    value = args.telstate['cbf_' + telstate_name]
    _logger.info('Setting %s to %s from telstate', attr_name, value)
    setattr(config, attr_name, value)

def _create_session_config(args):
    """Creates a SessionConfig object for a :class:`CaptureServer`.

    Note that this function makes blocking calls to telstate. The returned
    config has a blank filename.

    Parameters
    ----------
    args : :class:`argparse.Namespace`
        Command-line arguments. See :class:`CaptureServer`.
    """
    config = SessionConfig('')  # Real filename supplied later
    for endpoint in args.cbf_spead:
        config.add_endpoint(socket.gethostbyname(endpoint.host), endpoint.port)
    if args.interface is not None:
        config.interface_address = katsdpservices.get_interface_address(args.interface)
    config.ibv = args.ibv
    if args.affinity:
        config.disk_affinity = args.affinity[0]
        config.network_affinity = args.affinity[1]
    if args.direct_io:
        config.direct = True
    _config_from_telstate(args, config, 'ticks_between_spectra', 'ticks_between_spectra', False)
    _config_from_telstate(args, config, 'channels', 'n_chans')
    _config_from_telstate(args, config, 'channels_per_heap', 'n_chans_per_substream')
    _config_from_telstate(args, config, 'spectra_per_heap', 'spectra_per_heap')
    return config


class _CaptureSession(object):
    """Object encapsulating a co-routine that runs for a single capture session
    (from ``capture-init`` to end of stream or ``capture-done``.

    Parameters
    ----------
    config : SessionConfig
        Configuration generated by :meth:`create_session_config`
    telstate : :class:`katsdptelstate.TelescopeState`
        Telescope state (optional)
    loop : :class:`trollius.BaseEventLoop`
        IO Loop for the coroutine

    Attributes
    ----------
    filename : :class:`str`
        Filename of the HDF5 file written
    _telstate : :class:`katsdptelstate.TelescopeState`
        Telescope state interface, if any
    _loop : :class:`trollius.BaseEventLoop`
        Event loop passed to the constructor
    _session : :class:`katsdpingest._bf_ingest_session.Session`
        C++-driven capture session
    _run_future : :class:`trollius.Task`
        Task for the coroutine that waits for the C++ code and finalises
    """
    def __init__(self, config, telstate, loop):
        self._loop = loop
        self._telstate = telstate
        self.filename = config.filename
        self._session = Session(config)
        self._run_future = trollius.async(self._run(), loop=self._loop)

    def _write_metadata(self):
        telstate = self._telstate
        try:
            sync_time = telstate['cbf_sync_time']
            scale_factor_timestamp = telstate['cbf_scale_factor_timestamp']
            first_timestamp = sync_time + self._session.first_timestamp / scale_factor_timestamp
        except KeyError:
            _logger.warn('Failed to get timestamp conversion items, so skipping metadata')
            return
        antenna_mask = telstate.get('config', {}).get('antenna_mask', '').split(',')
        model = ar1_model.create_model(antenna_mask)
        model_data = telescope_model.TelstateModelData(model, telstate, first_timestamp)
        h5file = h5py.File(self.filename, 'r+')
        with contextlib.closing(h5file):
            file_writer.set_telescope_model(h5file, model_data)
            file_writer.set_telescope_state(h5file, telstate)

    @trollius.coroutine
    def _run(self):
        pool = concurrent.futures.ThreadPoolExecutor(1)
        try:
            yield From(self._loop.run_in_executor(pool, self._session.join))
            if self._session.n_heaps > 0:
                # Write the metadata to file
                self._write_metadata()
            _logger.info('Capture complete, %d heaps, of which %d dropped',
                         self._session.n_total_heaps,
                         self._session.n_total_heaps - self._session.n_heaps)
        except Exception as e:
            _logger.error("Capture threw exception", exc_info=True)

    @trollius.coroutine
    def stop(self):
        """Shut down the stream and wait for the session to end. This
        is a coroutine.
        """
        self._session.stop_stream()
        yield From(self._run_future)


class CaptureServer(object):
    """Beamformer capture. This contains all the core functionality of the
    katcp device server, without depending on katcp. It is split like this
    to facilitate unit testing.

    Parameters
    ----------
    args : :class:`argparse.Namespace`
        Command-line arguments. The following arguments are required. Refer to
        the script for documentation of these options.

        - cbf_spead
        - file_base
        - buffer
        - affinity
        - telstate
        - stream_name

    loop : :class:`trollius.BaseEventLoop`
        IO Loop for running coroutines

    Attributes
    ----------
    capturing : :class:`bool`
        Whether a capture session is in progress. Note that a session is
        considered to be in progress until explicitly stopped with
        :class:`stop_capture`, even if the stream has terminated.
    _args : :class:`argparse.Namespace`
        Command-line arguments passed to constructor
    _loop : :class:`trollius.BaseEventLoop`
        IO Loop passed to constructor
    _capture : :class:`_CaptureSession`
        Current capture session, or ``None`` if not capturing
    _config : :class:`katsdpingest.bf_ingest_session.SessionConfig`
        Configuration, with the filename to be filled in on capture-init
    """
    def __init__(self, args, loop):
        self._args = args
        self._loop = loop
        self._capture = None
        self._config = _create_session_config(args)

    @property
    def capturing(self):
        return self._capture is not None

    @trollius.coroutine
    def start_capture(self):
        """Start capture to file, if not already in progress.

        This is a co-routine.
        """
        if self._capture is None:
            basename = '{}.h5'.format(int(time.time()))
            self._config.filename = os.path.join(self._args.file_base, basename)
            self._capture = _CaptureSession(self._config, self._args.telstate, self._loop)
        raise Return(self._capture.filename)

    @trollius.coroutine
    def stop_capture(self):
        """Stop capture to file, if currently running. This is a co-routine."""
        if self._capture is not None:
            capture = self._capture
            yield From(capture.stop())
            # Protect against a concurrent stop and start changing to a new
            # capture.
            if self._capture is capture:
                self._capture = None


class KatcpCaptureServer(CaptureServer, katcp.DeviceServer):
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
    loop : :class:`trollius.BaseEventLoop`
        IO Loop for running coroutines
    """

    VERSION_INFO = ('bf-ingest', 1, 0)
    BUILD_INFO = ('katsdpingest',) + tuple(katsdpingest.__version__.split('.', 1)) + ('',)

    def __init__(self, args, loop):
        CaptureServer.__init__(self, args, loop)
        katcp.DeviceServer.__init__(self, args.host, args.port)

    def setup_sensors(self):
        pass

    @tornado.gen.coroutine
    def _start_capture(self):
        """Tornado variant of :meth:`start_capture`"""
        start_future = trollius.async(self.start_capture(), loop=self._loop)
        yield tornado.platform.asyncio.to_tornado_future(start_future)

    @request()
    @return_reply()
    @tornado.gen.coroutine
    def request_capture_init(self, sock):
        """Start capture to file."""
        if self.capturing:
            raise tornado.gen.Return(('fail', 'already capturing'))
        stat = os.statvfs(self._args.file_base)
        if stat.f_bavail / stat.f_blocks < 0.05:
            raise tornado.gen.Return(('fail', 'less than 5% disk space free on {}'.format(
                os.path.abspath(self._args.file_base))))
        yield self._start_capture()
        raise tornado.gen.Return(('ok',))

    @tornado.gen.coroutine
    def _stop_capture(self):
        """Tornado variant of :meth:`stop_capture`"""
        stop_future = trollius.async(self.stop_capture(), loop=self._loop)
        yield tornado.platform.asyncio.to_tornado_future(stop_future)

    @request()
    @return_reply()
    @tornado.gen.coroutine
    def request_capture_done(self, sock):
        """Stop a capture that is in progress."""
        if not self.capturing:
            raise tornado.gen.Return(('fail', 'not capturing'))
        yield self._stop_capture()
        raise tornado.gen.Return(('ok',))

    @tornado.gen.coroutine
    def stop(self):
        yield self._stop_capture()
        yield katcp.DeviceServer.stop(self)

    stop.__doc__ = katcp.DeviceServer.stop.__doc__


__all__ = ['CaptureServer', 'KatcpCaptureServer']
