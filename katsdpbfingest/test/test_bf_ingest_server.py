"""Tests for the bf_ingest_server module"""

from __future__ import print_function, division, absolute_import
import argparse
import h5py
import numpy as np
import mock
from katsdpingest import bf_ingest_server
from nose.tools import *
from katsdptelstate import endpoint
import trollius
from trollius import From
import spead2
import spead2.recv.trollius
import spead2.send


class MockFile(h5py.File):
    instance = None

    def __init__(self, *args, **kwargs):
        kwargs['driver'] = 'core'
        kwargs['backing_store'] = False
        super(MockFile, self).__init__(*args, **kwargs)
        MockFile.instance = self

    def close(self):
        pass

    def really_close(self):
        super(MockFile, self).close()
        MockFile.instance = None


class BaseTestCaptureSession(object):
    def setup(self):
        def add_buffer_reader(stream, *args, **kwargs):
            stream.add_buffer_reader(self.spead_data)

        self.spead_data = b''
        self.args = argparse.Namespace(
            cbf_channels=4096,
            cbf_spead=[endpoint.Endpoint('239.1.2.3', 7148)],
            file_base='/not_a_directory',
            buffer=self.buffer,
            affinity=None, interface=None, telstate=None)
        self.loop = trollius.get_event_loop()
        self._spead_patcher = mock.patch.object(
            spead2.recv.trollius.Stream, 'add_udp_reader', add_buffer_reader)
        self._h5py_patcher = mock.patch('h5py.File', MockFile)
        self._spead_patcher.start()
        self._h5py_patcher.start()

    def teardown(self):
        self._h5py_patcher.stop()
        self._spead_patcher.stop()
        if MockFile.instance:
            MockFile.instance.really_close()

    @trollius.coroutine
    def _test_manual_stop_no_data(self):
        server = bf_ingest_server.CaptureServer(self.args, self.loop)
        assert_false(server.capturing)
        server.start_capture()
        assert_true(server.capturing)
        yield From(trollius.sleep(0.01, loop=self.loop))
        yield From(server.stop_capture())
        assert_false(server.capturing)

    def test_manual_stop_no_data(self):
        """Manual stop before any data is received"""
        trollius.get_event_loop().run_until_complete(self._test_manual_stop_no_data())

    @trollius.coroutine
    def _test_stream(self, end):
        n_channels = 768
        n_time = 256
        n_heaps = 5
        # Prepare a SPEAD stream
        config = spead2.send.StreamConfig(max_packet_size=4196)
        flavour = spead2.Flavour(4, 64, 48, 0)
        stream = spead2.send.BytesStream(spead2.ThreadPool(), config)
        ig = spead2.send.ItemGroup(flavour=flavour)
        ig.add_item(name='timestamp', id=0x1600,
                    description='Timestamp', shape=(), format=[('u', 48)])
        ig.add_item(name='bf_raw',  id=0x5000,
                    description='Beamformer data', shape=(n_channels, n_time, 2), dtype=np.int8)
        stream.send_heap(ig.get_heap())
        stream.send_heap(ig.get_start())
        ts = 1234567890
        for i in range(5):
            ig['timestamp'].value = ts
            ig['bf_raw'].value = np.zeros((n_channels, n_time, 2), np.int8)
            for channel in range(n_channels):
                ig['bf_raw'].value[channel, :, 0] = channel % 255 - 128
            for time in range(n_time):
                ig['bf_raw'].value[:, time, 1] = (i * n_time + time) % 255 - 128
            stream.send_heap(ig.get_heap())
            ts += n_time * (2 * self.args.cbf_channels)
        if end:
            stream.send_heap(ig.get_end())
        self.spead_data = stream.getvalue()
        stream = None

        # Feed it to the server
        server = bf_ingest_server.CaptureServer(self.args, self.loop)
        server.start_capture()
        # Wait for stream to shut down on its own
        if end:
            yield From(server._capture._run_future)
        else:
            yield From(trollius.sleep(0.1))
        # Shut down the session
        yield From(server.stop_capture())

        # Validate the output
        h5file = MockFile.instance
        bf_raw = h5file['/Data/bf_raw']
        expected = np.empty_like(bf_raw)
        for channel in range(n_channels):
            expected[channel, :, 0] = channel % 255 - 128
        for time in range(n_time * n_heaps):
            expected[:, time, 1] = time % 255 - 128
        np.testing.assert_equal(expected, bf_raw)

        timestamp = h5file['/Data/timestamps']
        expected = 1234567890 + 2 * self.args.cbf_channels * np.arange(n_time * n_heaps)
        np.testing.assert_equal(expected, timestamp)
        h5file.really_close()

    def test_stream_end(self):
        """Stream ends with an end-of-stream"""
        trollius.get_event_loop().run_until_complete(self._test_stream(True))

    def test_stream_no_end(self):
        """Stream ends with a stop request"""
        trollius.get_event_loop().run_until_complete(self._test_stream(False))


class TestCaptureSessionNoBuffer(BaseTestCaptureSession):
    buffer = False


class TestCaptureSessionBuffer(BaseTestCaptureSession):
    buffer = True
