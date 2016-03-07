from __future__ import print_function, division, absolute_import
import spead2
import spead2.recv
import spead2.recv.trollius
import katcp
from katcp.kattypes import request, return_reply
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


_logger = logging.getLogger(__name__)


class _CaptureSession(object):
    def __init__(self, args, loop):
        self._loop = loop
        self._args = args
        self._file = None
        self._bf_raw_dataset = None
        self._timestamps = []
        self._manual_stop = False
        if args.cbf_channels:
            self._timestep = 2 * args.cbf_channels
        else:
            self._timestep = None
        self._ig = spead2.ItemGroup()
        self._stream = spead2.recv.trollius.Stream(spead2.ThreadPool(), 0, loop=self._loop)
        for endpoint in args.cbf_spead:
            _logger.info('Listening on endpoint {}'.format(endpoint))
            self._stream.add_udp_reader(endpoint.port, bind_hostname=endpoint.host)
        self._run_future = trollius.async(self._run(), loop=self._loop)

    def _create_file(self):
        filename = os.path.join(self._args.file_base, '{}.h5'.format(int(time.time())))
        # Allocate enough cache space for 3 chunks - should be enough for one active, one
        # being written back, and a bit extra for timestamps.
        n_chans, n_time = self._ig['bf_raw'].shape[0:2]
        dtype = self._ig['bf_raw'].dtype
        cache_size = 4 * n_time * n_chans * 2 * dtype.itemsize
        self._file = h5py.File(filename, mode='w')
        self._file.attrs['version'] = 1
        data_group = self._file.create_group('Data')
        shape = (n_chans, 0, 2)
        maxshape = (n_chans, None, 2)
        self._bf_raw_dataset = data_group.create_dataset(
            'bf_raw', shape, maxshape=maxshape, dtype=dtype, chunks=(n_chans, n_time, 2))
        if self._timestep is None:
            _logger.info('Assuming %d PFB channels; if not, pass --cbf-channels', n_chans)
            self._timestep = 2 * n_chans

    @trollius.coroutine
    def _run(self):
        try:
            data_heaps = 0
            while True:
                heap = yield From(self._stream.get())
                updated = self._ig.update(heap)
                is_data_heap = 'timestamp' in updated and 'bf_raw' in updated
                if (not self._file
                    and (heap.is_start_of_stream() or is_data_heap)
                    and 'bf_raw' in self._ig):
                    self._create_file()
                if not is_data_heap:
                    _logger.info('Received non-data heap %d', heap.cnt)
                    continue
                timestamp = self._ig['timestamp'].value
                bf_raw = self._ig['bf_raw'].value
                _logger.debug('Received heap with timestamp %d', timestamp)

                n_chans, n_time = bf_raw.shape[0:2]
                if n_chans != self._bf_raw_dataset.shape[0]:
                    _logger.warning('Dropping heap because number of channels does not match')
                    continue
                idx = self._bf_raw_dataset.shape[1]
                self._bf_raw_dataset.resize(idx + n_time, axis=1)
                self._bf_raw_dataset[:, idx : idx + n_time, :] = bf_raw
                self._timestamps.append(timestamp)
                data_heaps += 1
                # Check free space periodically, but every heap is excessive
                if data_heaps % 100 == 0:
                    _logger.info('Received %d data heaps', data_heaps)
                    stat = os.statvfs(self._file.filename)
                    free_bytes = stat.f_frsize * stat.f_bavail
                    if free_bytes < 300 * bf_raw.nbytes + 8 * n_time * data_heaps:
                        _logger.warn('Capture stopped due to lack of disk space')
                        break
        except spead2.Stopped:
            if self._manual_stop:
                _logger.info('Capture terminated by request')
            else:
                _logger.info('Capture terminated by stop heap')
        except Exception:
            _logger.error('Capture coroutine threw uncaught exception', exc_info=True)
        finally:
            self._stream.stop()
            if self._file:
                n_time = self._ig['bf_raw'].shape[1]
                ds = self._file['Data'].create_dataset(
                    'timestamp',
                    shape=(n_time * len(self._timestamps),),
                    dtype=np.uint64)
                idx = 0
                for timestamp in self._timestamps:
                    ds[idx : idx + n_time] = np.arange(
                        timestamp, timestamp + n_time * self._timestep, self._timestep,
                        dtype=np.uint64)
                    idx += n_time
                self._file.close()
                self._file = None

    @trollius.coroutine
    def stop(self):
        self._manual_stop = True
        self._stream.stop()
        yield From(self._run_future)


class CaptureServer(katcp.DeviceServer):
    """katcp device server for beamformer simulation"""
    VERSION_INFO = ('bf-ingest', 1, 0)
    BUILD_INFO = ('katsdpingest',) + tuple(katsdpingest.__version__.split('.', 1)) + ('',)

    def __init__(self, args, loop):
        super(CaptureServer, self).__init__(args.host, args.port)
        self._args = args
        self._loop = loop
        self._capture = None

    def setup_sensors(self):
        pass

    @request()
    @return_reply()
    def request_capture_init(self, sock):
        """Start capture to file"""
        if self._capture is not None:
            return ('fail', 'already capturing')
        self._capture = _CaptureSession(self._args, self._loop)
        return ('ok',)

    @tornado.gen.coroutine
    def _stop_capture(self):
        if self._capture is not None:
            stop_future = trollius.async(self._capture.stop(), loop=self._loop)
            yield tornado.platform.asyncio.to_tornado_future(stop_future)
            self._capture = None

    @request()
    @return_reply()
    @tornado.gen.coroutine
    def request_capture_done(self, sock):
        """Stop a capture that is in progress"""
        if self._capture is None:
            raise tornado.gen.Return(('fail', 'not capturing'))
        yield self._stop_capture()
        raise tornado.gen.Return(('ok',))

    @tornado.gen.coroutine
    def stop(self):
        yield self._stop_capture()
        yield super(CaptureServer, self).stop()

__all__ = ['CaptureServer']
