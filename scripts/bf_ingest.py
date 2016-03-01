#!/usr/bin/env python

from __future__ import print_function, division, absolute_import
import spead2
import spead2.recv
import spead2.recv.trollius
import katcp
from katcp.kattypes import request, return_reply
import h5py
import numpy as np
import time
import signal
import os.path
import trollius
import tornado
from tornado.platform.asyncio import AsyncIOMainLoop, to_asyncio_future
import logging
from trollius import From, Return
import argparse
import katsdptelstate.endpoint
import katversion


_logger = logging.getLogger('')


def _make_fapl(cache_entries, cache_size, w0):
    """Create a File Access Properties List for h5py with a specified number
    of cache entries and cache size. This is based around the internal
    make_fapl function in h5py.
    """

    fapl = h5py.h5p.create(h5py.h5p.FILE_ACCESS)
    cache_settings = list(fapl.get_cache())
    fapl.set_cache(cache_settings[0], cache_entries, cache_size, w0)
    fapl.set_fclose_degree(h5py.h5f.CLOSE_STRONG)
    fapl.set_libver_bounds(h5py.h5f.LIBVER_LATEST, h5py.h5f.LIBVER_LATEST)
    return fapl


def _transpose2(a, out=None):
    """Transpose a 2D array of complex numbers, represented as a 3D array with
    the last dimension being of size 2. This is written in a rather nasty way
    in order to hit a fast path in numpy, giving about 5-10 times speedup over
    just using np.swapaxes.
    """
    view_dtype = np.dtype(a.dtype, (2,))
    if out is None:
        out = np.empty((a.shape[1], a.shape[0], 2), a.dtype)
    a_view = a.view(view_dtype)[:, :, 0]
    out_view = out.view(view_dtype)[:, :, 0]
    out_view[:] = a_view.T


class CaptureSession(object):
    def __init__(self, args, loop):
        self._loop = loop
        self._args = args
        self._file = None
        self._bf_raw_dataset = None
        self._timestamp_dataset = None
        self._transposed = None
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
        # HDF5 recommendation is for ~100 slots per chunk, and a prime number
        fapl = _make_fapl(401, cache_size, 0.99)
        self._file = h5py.File(filename, mode='w')
        self._file.attrs['version'] = 1
        data_group = self._file.create_group('Data')
        shape = (0, n_chans, 2)
        maxshape = (None, n_chans, 2)
        self._bf_raw_dataset = data_group.create_dataset(
            'bf_raw', shape, maxshape=maxshape, dtype=dtype, chunks=(n_time, n_chans, 2))
        self._timestamp_dataset = data_group.create_dataset('timestamp', (0,), maxshape=(None,), dtype=np.uint64)
        if self._timestep is None:
            _logger.info('Assuming %d PFB channels; if not, pass --cbf-channels', n_chans)
            self._timestep = 2 * n_chans
        self._transposed = np.empty((n_time, n_chans, 2), dtype)

    def _process_heap(self, heap):
        updated = self._ig.update(heap)
        is_data_heap = 'timestamp' in updated and 'bf_raw' in updated
        if (not self._file
            and (heap.is_start_of_stream() or is_data_heap)
            and 'bf_raw' in self._ig):
            self._create_file()
        if not is_data_heap:
            _logger.info('Received non-data heap %d', heap.cnt)
            return
        timestamp = self._ig['timestamp'].value
        bf_raw = self._ig['bf_raw'].value
        _logger.info('Received heap with timestamp %d', timestamp)

        n_chans, n_time = bf_raw.shape[0:2]
        if n_chans != self._bf_raw_dataset.shape[1]:
            _logger.warning('Dropping heap because number of channels does not match')
            return
        idx = self._bf_raw_dataset.shape[0]
        self._bf_raw_dataset.resize(idx + n_time, axis=0)
        self._timestamp_dataset.resize(idx + n_time, axis=0)
        # bf_raw is in channel-time-component order; we want time-channel-component
        _transpose2(bf_raw, self._transposed)
        self._bf_raw_dataset[idx : idx + n_time, ...] = self._transposed
        timestamps = np.arange(timestamp,
                               timestamp + self._timestep * n_time,
                               self._timestep,
                               dtype=np.uint64)
        self._timestamp_dataset[idx : idx + n_time] = timestamps

    @trollius.coroutine
    def _run(self):
        try:
            while True:
                heap = yield From(self._stream.get())
                self._process_heap(heap)
        except spead2.Stopped:
            if self._manual_stop:
                _logger.info('Capture terminated by request')
            else:
                _logger.info('Capture terminated by stop heap')
        except Exception:
            _logger.error('Capture coroutine threw uncaught exception', exc_info=True)
        finally:
            if self._file:
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
    BUILD_INFO = ('katsdpingest',) + tuple(katversion.get_version(__file__).split('.', 1)) + ('',)

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
        self._capture = CaptureSession(self._args, self._loop)
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


@trollius.coroutine
def on_shutdown(server):
    _logger.info('Shutting down')
    yield From(to_asyncio_future(server.stop()))
    trollius.get_event_loop().stop()


def main():
    parser = katsdptelstate.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--cbf-channels', type=int, help='total number of PFB channels [defaults to number of channels in stream]')
    parser.add_argument('--cbf-spead', type=katsdptelstate.endpoint.endpoint_list_parser(7148), default=':7148', help='endpoints to listen for CBF SPEAD stream (including multicast IPs). [<ip>[+<count>]][:port].', metavar='ENDPOINTS')
    parser.add_argument('--logging', '-l', type=str, metavar='LEVEL', default='INFO', help='log level')
    parser.add_argument('--file-base', default='.', type=str, help='base directory into which to write HDF5 files', metavar='DIR')
    parser.add_argument('--port', '-p', type=int, default=2050, help='katcp host port')
    parser.add_argument('--host', '-a', type=str, default='', help='katcp host address')
    args = parser.parse_args()
    logging.basicConfig(level=args.logging, format='%(asctime)s %(levelname)s:%(name)s: %(message)s')

    ioloop = AsyncIOMainLoop()
    ioloop.install()
    server = CaptureServer(args, trollius.get_event_loop())
    server.set_concurrency_options(thread_safe=False, handler_thread=False)
    server.set_ioloop(ioloop)
    trollius.get_event_loop().add_signal_handler(signal.SIGINT,
        lambda: trollius.async(on_shutdown(server)))
    trollius.get_event_loop().add_signal_handler(signal.SIGTERM,
        lambda: trollius.async(on_shutdown(server)))
    ioloop.add_callback(server.start)
    trollius.get_event_loop().run_forever()


if __name__ == '__main__':
    main()
