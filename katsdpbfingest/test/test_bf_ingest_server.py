"""Tests for the bf_ingest_server module"""

from __future__ import print_function, division, absolute_import
import argparse
import tempfile
import shutil
import os.path
import time
import contextlib
import socket

import h5py
import numpy as np

import trollius
from trollius import From

import spead2
import spead2.recv.trollius
import spead2.send

from nose.tools import assert_equal, assert_true, assert_false

import katsdptelstate
from katsdptelstate import endpoint

from katsdpbfingest import bf_ingest_server, _bf_ingest_session
from ..utils import Range


class TestSession(object):
    def setup(self):
        # To avoid collisions when running tests in parallel on a single host,
        # create a socket for the duration of the test and use its port as the
        # port for the test. Sockets in the same network namespace should have
        # unique ports.
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind(('127.0.0.1', 0))
        self.port = self._sock.getsockname()[1]
        self.tmpdir = tempfile.mkdtemp()

    def teardown(self):
        shutil.rmtree(self.tmpdir)
        self._sock.close()

    def test_no_stop(self):
        """Deleting a session without stopping it must tidy up"""
        config = _bf_ingest_session.SessionConfig(os.path.join(self.tmpdir, 'test_no_stop.h5'))
        config.add_endpoint('239.1.2.3', self.port)
        config.channels = 4096
        config.channels_per_heap = 256
        config.spectra_per_heap = 256
        config.ticks_between_spectra = 8192
        _bf_ingest_session.Session(config)


class TestCaptureServer(object):
    def setup(self):
        self.tmpdir = tempfile.mkdtemp()
        # To avoid collisions when running tests in parallel on a single host,
        # create a socket for the duration of the test and use its port as the
        # port for the test. Sockets in the same network namespace should have
        # unique ports.
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind(('127.0.0.1', 0))
        self.port = self._sock.getsockname()[1]
        self.n_channels = 1024
        self.spectra_per_heap = 256
        self.endpoints = endpoint.endpoint_list_parser(self.port)(
            '239.102.2.0+7:{}'.format(self.port))
        self.n_bengs = 16
        self.ticks_between_spectra = 8192
        self.channels_per_heap = self.n_channels // self.n_bengs
        attrs = {
            'i0_tied_array_channelised_voltage_0x_n_chans': self.n_channels,
            'i0_tied_array_channelised_voltage_0x_n_chans_per_substream': self.channels_per_heap,
            'i0_tied_array_channelised_voltage_0x_spectra_per_heap': self.spectra_per_heap,
            'i0_tied_array_channelised_voltage_0x_src_streams': [
                'i0_antenna_channelised_voltage'],
            'i0_antenna_channelised_voltage_ticks_between_spectra': self.ticks_between_spectra,
            'i0_antenna_channelised_voltage_instrument_dev_name': 'i0',
            'i0_sync_time': 123456789.0,
            'i0_scale_factor_timestamp': 1712000000.0
        }
        telstate = katsdptelstate.TelescopeState()
        for key, value in attrs.items():
            telstate.add(key, value, immutable=True)
        self.args = argparse.Namespace(
            cbf_spead=self.endpoints,
            channels=Range(128, 768),
            file_base=self.tmpdir,
            direct_io=False,
            ibv=False,
            stream_name='i0_tied_array_channelised_voltage_0x',
            affinity=None,
            interface='lo',
            telstate=telstate)
        self.loop = trollius.get_event_loop()

    def teardown(self):
        shutil.rmtree(self.tmpdir)
        self._sock.close()
        self.args.telstate.clear()

    @trollius.coroutine
    def _test_manual_stop_no_data(self):
        server = bf_ingest_server.CaptureServer(self.args, self.loop)
        assert_false(server.capturing)
        yield From(server.start_capture('1122334455'))
        assert_true(server.capturing)
        yield From(trollius.sleep(0.01, loop=self.loop))
        yield From(server.stop_capture())
        assert_false(server.capturing)

    def test_manual_stop_no_data(self):
        """Manual stop before any data is received"""
        trollius.get_event_loop().run_until_complete(self._test_manual_stop_no_data())

    @trollius.coroutine
    def _test_stream(self, end):
        n_heaps = 25              # number of heaps in time
        n_spectra = self.spectra_per_heap * n_heaps
        # Pick some heaps to drop, including an entire slice
        drop = np.zeros((self.n_bengs, n_heaps), np.bool_)
        drop[:, 4] = True
        drop[2, 9] = True
        drop[7, 24] = True

        # Start up the server
        server = bf_ingest_server.CaptureServer(self.args, self.loop)
        filename = yield From(server.start_capture('1122334455'))
        time.sleep(0.1)
        # Send it a SPEAD stream
        config = spead2.send.StreamConfig(max_packet_size=4196, rate=1e9 / 8)
        flavour = spead2.Flavour(4, 64, 48, 0)
        ig = spead2.send.ItemGroup(flavour=flavour)
        ig.add_item(name='timestamp', id=0x1600,
                    description='Timestamp', shape=(), format=[('u', 48)])
        ig.add_item(name='frequency', id=0x4103,
                    description='The frequency channel of the data in this HEAP.',
                    shape=(), format=[('u', 48)])
        ig.add_item(name='bf_raw', id=0x5000,
                    description='Beamformer data',
                    shape=(self.channels_per_heap, self.spectra_per_heap, 2), dtype=np.int8)
        streams = [spead2.send.UdpStream(
            spead2.ThreadPool(), ep.host, self.port, config, ttl=1, interface_address='127.0.0.1')
            for ep in self.endpoints]
        for stream in streams:
            stream.send_heap(ig.get_heap(descriptors='all'))
            stream.send_heap(ig.get_start())
        ts = 1234567890
        for i in range(n_heaps):
            data = np.zeros((self.n_channels, self.spectra_per_heap, 2), np.int8)
            for channel in range(self.n_channels):
                data[channel, :, 0] = channel % 255 - 128
            for t in range(self.spectra_per_heap):
                data[:, t, 1] = (i * self.spectra_per_heap + t) % 255 - 128
            for j in range(self.n_bengs):
                ig['timestamp'].value = ts
                ig['frequency'].value = j * self.channels_per_heap
                ig['bf_raw'].value = data[j * self.channels_per_heap
                                          : (j + 1) * self.channels_per_heap, ...]
                if not drop[j, i]:
                    streams[j // (self.n_bengs // len(self.endpoints))].send_heap(ig.get_heap())
            ts += self.spectra_per_heap * self.ticks_between_spectra
        if end:
            for stream in streams:
                stream.send_heap(ig.get_end())
        streams = []

        # Wait for stream to shut down on its own
        yield From(trollius.sleep(0.1))
        # Shut down the session
        yield From(server.stop_capture())

        # Validate the output
        h5file = h5py.File(filename, 'r')
        with contextlib.closing(h5file):
            bf_raw = h5file['/Data/bf_raw']
            expected = np.zeros((self.n_channels, n_spectra, 2), np.int8)
            for channel in range(self.n_channels):
                expected[channel, :, 0] = channel % 255 - 128
            for t in range(n_spectra):
                expected[:, t, 1] = t % 255 - 128
            for i in range(self.n_bengs):
                for j in range(n_heaps):
                    if drop[i, j]:
                        channel0 = i * self.channels_per_heap
                        spectrum0 = j * self.spectra_per_heap
                        expected[channel0 : channel0 + self.channels_per_heap,
                                 spectrum0 : spectrum0 + self.spectra_per_heap, :] = 0
            expected = expected[self.args.channels.asslice()]
            np.testing.assert_equal(expected, bf_raw)

            timestamps = h5file['/Data/timestamps']
            expected = 1234567890 \
                + self.ticks_between_spectra * np.arange(self.spectra_per_heap * n_heaps)
            np.testing.assert_equal(expected, timestamps)

            captured_timestamps = h5file['/Data/captured_timestamps']
            slice_drops = np.sum(drop, axis=0)
            captured_slices = np.nonzero(slice_drops == 0)[0]
            captured_spectra = captured_slices[:, np.newaxis] * self.spectra_per_heap + \
                np.arange(self.spectra_per_heap)[np.newaxis, :]
            expected = 1234567890 + self.ticks_between_spectra * captured_spectra.flatten()
            np.testing.assert_equal(expected, captured_timestamps)

            flags = h5file['/Data/flags']
            expected = np.where(drop, 8, 0).astype(np.uint8)
            expected = expected[self.args.channels.start // self.channels_per_heap :
                                self.args.channels.stop // self.channels_per_heap]
            np.testing.assert_equal(expected, flags)

            data = h5file['/Data']
            assert_equal('i0_tied_array_channelised_voltage_0x', data.attrs['stream_name'])
            assert_equal(self.args.channels.start, data.attrs['channel_offset'])

    def test_stream_end(self):
        """Stream ends with an end-of-stream"""
        trollius.get_event_loop().run_until_complete(self._test_stream(True))

    def test_stream_no_end(self):
        """Stream ends with a stop request"""
        trollius.get_event_loop().run_until_complete(self._test_stream(False))
