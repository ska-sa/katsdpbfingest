from __future__ import print_function, division, absolute_import
import spead2
import spead2.recv
import spead2.recv.trollius
import katcp
from katcp.kattypes import request, return_reply
from katsdpfilewriter import telescope_model, ar1_model, file_writer
import h5py
import numpy as np
import time
import os
import os.path
import trollius
from trollius import From
import tornado
import logging
import katsdpingest
import psutil
import ipaddress
import netifaces
import socket


_logger = logging.getLogger(__name__)


def _get_interface_address(interface):
    return netifaces.ifaddresses(interface)[netifaces.AF_INET][0]['addr']


class _CaptureSession(object):
    """Object encapsulating a co-routine that runs for a single capture session
    (from ``capture-init`` to end of stream or ``capture-done``.

    Parameters
    ----------
    args : :class:`argparse.Namespace`
        Command-line arguments. See :class:`CaptureServer`.
    loop : :class:`trollius.BaseEventLoop`
        IO Loop for the coroutine

    Attributes
    ----------
    _args : :class:`argparse.Namespace`
        Command-line arguments passed to the constructor
    _loop : :class:`trollius.BaseEventLoop`
        Event loop passed to the constructor
    _file : :class:`h5py.File`
        Handle to HDF5 file
    _bf_raw_dataset : :class:`h5py.Dataset`
        Handle to the ``bf_raw`` dataset
    _bf_raw : :class:`list`
        `bf_raw` numpy arrays received when in buffering mode
    _timestamps : :class:`list`
        Timestamp for the start of each *received* heap
    _manual_stop : :class:`bool`
        Whether :meth:`stop` has been called
    _ig : :class:`spead2.ItemGroup`
        Item group updated with each heap
    _stream : :class:`spead2.recv.trollius.Stream`
        SPEAD stream for incoming data
    _run_future : :class:`trollius.Task`
        Task for the coroutine that does the work
    _first_timestamp : int
        Timestamp (as ADC counter) for first data heap
    _timestep : int
        Time interval (in ADC clocks) between spectra
    """
    def __init__(self, args, loop):
        self._loop = loop
        self._args = args
        self._file = None
        self._bf_raw_dataset = None
        self._bf_raw = []
        self._timestamps = []
        self._manual_stop = False
        if args.cbf_channels:
            self._timestep = 2 * args.cbf_channels
        else:
            self._timestep = None
        self._first_timestamp = None
        self._ig = spead2.ItemGroup()
        if args.affinity:
            spead2.ThreadPool.set_affinity(args.affinity[0])
            thread_pool = spead2.ThreadPool(1, [args.affinity[1]])
        else:
            thread_pool = spead2.ThreadPool()
        self._stream = spead2.recv.trollius.Stream(thread_pool, 0, ring_heaps=16, loop=self._loop)
        for endpoint in args.cbf_spead:
            _logger.info('Listening on endpoint {}'.format(endpoint))
            # Buffer size is increased from the default, because doing so seems to prevent some
            # occasional dropped heaps
            interface_address = None
            if endpoint.host is not None and args.interface is not None:
                address = socket.gethostbyname(endpoint.host)
                # ipaddress module requires a unicode string
                address = ipaddress.ip_address(unicode(address, 'us-ascii'))
                if address.is_multicast:
                    interface_address = _get_interface_address(args.interface)
            if interface_address is not None:
                self._stream.add_udp_reader(endpoint.host, endpoint.port, buffer_size=16*1024*1024,
                                            interface_address=interface_address)
            else:
                self._stream.add_udp_reader(endpoint.port, bind_hostname=endpoint.host,
                                            buffer_size=16*1024*1024)
        self._run_future = trollius.async(self._run(), loop=self._loop)

    def _create_file_and_memory_pool(self):
        """Create the HDF5 file and dataset. This is called once the data shape
        is known.
        """
        filename = os.path.join(self._args.file_base, '{}.h5'.format(int(time.time())))
        _logger.info('Creating file %s', filename)
        # Allocate enough cache space for 3 chunks - should be enough for one active, one
        # being written back, and a bit extra for timestamps.
        n_chans, n_time = self._ig['bf_raw'].shape[0:2]
        dtype = self._ig['bf_raw'].dtype
        self._file = h5py.File(filename, mode='w')
        self._file.attrs['version'] = 2
        data_group = self._file.create_group('Data')
        shape = (n_chans, 0, 2)
        maxshape = (n_chans, None, 2)
        self._bf_raw_dataset = data_group.create_dataset(
            'bf_raw', shape, maxshape=maxshape, dtype=dtype, chunks=(n_chans, n_time, 2))
        if self._timestep is None:
            _logger.info('Assuming %d PFB channels; if not, pass --cbf-channels', n_chans)
            self._timestep = 2 * n_chans
        chunk_size = n_time * n_chans * 2 * dtype.itemsize
        if self._args.buffer:
            if self._args.affinity:
                thread_pool = spead2.ThreadPool(1, [self._args.affinity[2]])
            else:
                thread_pool = spead2.ThreadPool()
            memory_pool = spead2.MemoryPool(thread_pool, chunk_size, chunk_size + 4096, 16, 8, 7)
        else:
            memory_pool = spead2.MemoryPool(chunk_size, chunk_size + 4096, 24, 16)
        self._stream.set_memory_pool(memory_pool)

    def _write_heap(self, timestamp, bf_raw):
        n_time = bf_raw.shape[1]
        idx = (timestamp - self._first_timestamp) // self._timestep
        if idx < 0:
            _logger.warning('Discarding heap that pre-dates the initial timestamp')
            return True
        if idx + n_time > self._bf_raw_dataset.shape[1]:
            self._bf_raw_dataset.resize(idx + n_time, axis=1)
        self._bf_raw_dataset[:, idx : idx + n_time, :] = bf_raw
        self._timestamps.append(timestamp)
        # Check free space periodically, but every heap is excessive
        n_heaps = idx // n_time + 1
        if n_heaps % 100 == 0:
            _logger.info('Processed %d data heaps', n_heaps)
            stat = os.statvfs(self._file.filename)
            free_bytes = stat.f_frsize * stat.f_bavail
            # We check only every 100 dumps, so we need enough space for
            # the remaining dumps, plus all the timestamps. We also leave
            # a fixed amount free to ensure that there is room for overheads.
            if free_bytes < 1024**3 + 100 * bf_raw.nbytes + 8 * n_time * n_heaps:
                _logger.warn('Processing stopped due to lack of disk space')
                return False
        return True

    def _process_heap(self, heap):
        """
        Apply processing for a single heap.

        Returns
        -------
        bool
            True to continue, False if processing should stop due to lack of space
        """
        updated = self._ig.update(heap)
        is_data_heap = 'timestamp' in updated and 'bf_raw' in updated
        # Check whether we need to, and can, create the file. We need
        # the descriptor for bf_raw, but CBF is also known to send
        # multiple versions of some metadata, so we wait for an
        # indication that the metadata is complete.
        if (not self._file and
                (heap.is_start_of_stream() or is_data_heap) and
                'bf_raw' in self._ig):
            self._create_file_and_memory_pool()
        if not is_data_heap:
            _logger.info('Received non-data heap %d', heap.cnt)
            return True
        timestamp = self._ig['timestamp'].value
        bf_raw = self._ig['bf_raw'].value
        _logger.debug('Received heap with timestamp %d', timestamp)
        if self._first_timestamp is None:
            self._first_timestamp = timestamp

        n_chans = bf_raw.shape[0]
        if n_chans != self._bf_raw_dataset.shape[0]:
            _logger.warning('Dropping heap because number of channels does not match')
            return True
        if self._args.buffer:
            self._bf_raw.append(bf_raw)
            self._timestamps.append(timestamp)
            n_heaps = len(self._bf_raw)
            if n_heaps % 100 == 0:
                _logger.info('Received %d heaps', n_heaps)
                # Since we only check every 100 dumps, we need to have enough
                # memory for another 100 dumps, plus we leave a decent amount
                # free so that the system doesn't start paging too much out.
                if psutil.virtual_memory().available < 2 * 1024**3 + 100 * bf_raw.nbytes:
                    _logger.warn('Capture terminated due to lack of memory')
                    return False
            return True
        else:
            return self._write_heap(timestamp, bf_raw)

    def _write_metadata(self):
        telstate = self._args.telstate
        antenna_mask = telstate.get('config', {}).get('antenna_mask', '').split(',')
        model = ar1_model.create_model(antenna_mask)
        model_data = telescope_model.TelstateModelData(model, telstate, self._timestamps[0])
        file_writer.set_telescope_model(self._file, model_data)
        file_writer.set_telescope_state(self._file, telstate)

    def _finalise(self):
        self._stream.stop()
        if self._file:
            # In buffering mode, write the data to file
            # Take a copy of the original timestamps, because _write_heap will
            # record the timestamps that actually get written to file.
            if self._args.buffer:
                orig_timestamps = self._timestamps
                self._timestamps = []
                for timestamp, bf_raw in zip(orig_timestamps, self._bf_raw):
                    if not self._write_heap(timestamp, bf_raw):
                        break
            # Write the timestamps of captured data to file
            n_time = self._ig['bf_raw'].shape[1]
            self._timestamps.sort()
            ds = self._file['Data'].create_dataset(
                'captured_timestamps',
                shape=(n_time * len(self._timestamps),),
                dtype=np.uint64)
            ds.attrs['timestamp_reference'] = 'start'
            ds.attrs['timestamp_type'] = 'adc'
            idx = 0
            for timestamp in self._timestamps:
                ds[idx : idx + n_time] = np.arange(
                    timestamp, timestamp + n_time * self._timestep, self._timestep,
                    dtype=np.uint64)
                idx += n_time
            # Write full set of timestamps (for captured and padded data)
            n_spectra = self._bf_raw_dataset.shape[1]
            ds = self._file['Data'].create_dataset(
                'timestamps', shape=(n_spectra,), dtype=np.uint64)
            ds.attrs['timestamp_reference'] = 'start'
            ds.attrs['timestamp_type'] = 'adc'
            ds[:] = np.arange(self._first_timestamp,
                              self._first_timestamp + n_spectra * self._timestep,
                              self._timestep,
                              dtype=np.uint64)
            # Write the metadata to file
            if self._args.telstate is not None and self._timestamps:
                self._write_metadata()
            self._file.close()
            self._file = None

        if self._timestamps:
            elapsed = max(self._timestamps) - min(self._timestamps)
            expected_heaps = elapsed // (n_time * self._timestep) + 1
        else:
            expected_heaps = 0
        n_heaps = len(self._timestamps)
        _logger.info('Received %d heaps, expected %d based on min/max timestamps',
                     n_heaps, expected_heaps)
        if n_heaps < expected_heaps:
            _logger.warn('%d heaps missing', expected_heaps - n_heaps)
        elif n_heaps > expected_heaps:
            _logger.warn('%d more heaps than expected (timestamp errors?)',
                         n_heaps - expected_heaps)
        _logger.info('Capture complete')

    @trollius.coroutine
    def _run(self):
        """Does the work of capturing a stream. This is a coroutine."""
        try:
            try:
                while True:
                    heap = yield From(self._stream.get())
                    if not self._process_heap(heap):
                        break
            except spead2.Stopped:
                if self._manual_stop:
                    _logger.info('Capture terminated by request')
                else:
                    _logger.info('Capture terminated by stop heap')
            except Exception:
                _logger.error('Capture coroutine threw uncaught exception', exc_info=True)
                raise
        finally:
            try:
                self._finalise()
            except Exception:
                _logger.error('Capture coroutine threw uncaught exception while finalising', exc_info=True)
                raise

    @trollius.coroutine
    def stop(self):
        """Shut down the stream and wait for the coroutine to complete. This
        is a coroutine.
        """
        self._manual_stop = True
        self._stream.stop()
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

        - cbf_channels
        - cbf_spead
        - file_base
        - buffer
        - affinity

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
    """
    def __init__(self, args, loop):
        self._args = args
        self._loop = loop
        self._capture = None

    @property
    def capturing(self):
        return self._capture is not None

    def start_capture(self):
        """Start capture to file, if not already in progress."""
        if self._capture is None:
            self._capture = _CaptureSession(self._args, self._loop)

    @trollius.coroutine
    def stop_capture(self):
        """Stop capture to file, if currently running. This is a co-routine."""
        if self._capture is not None:
            yield From(self._capture.stop())
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

    @request()
    @return_reply()
    def request_capture_init(self, sock):
        """Start capture to file."""
        if self.capturing:
            return ('fail', 'already capturing')
        stat = os.statvfs(self._args.file_base)
        if stat.f_bavail / stat.f_blocks < 0.05:
            return ('fail', 'less than 5% disk space free on {}'.format(os.path.abspath(self._args.file_base)))
        self.start_capture()
        return ('ok',)

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
