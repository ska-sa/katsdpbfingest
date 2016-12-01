"""Tests for the bf_ingest_server module"""

from __future__ import print_function, division, absolute_import
import argparse
import h5py
import tempfile
import shutil
import time
import contextlib
import numpy as np
from katsdpingest import bf_ingest_server, _bf_ingest_session
from nose.tools import *
from katsdptelstate import endpoint
import trollius
from trollius import From
import spead2
import spead2.recv.trollius
import spead2.send
import socket


class TestSession(object):
    def setup(self):
        # To avoid collisions when running tests in parallel on a single host,
        # create a socket for the duration of the test and use its port as the
        # port for the test. Sockets in the same network namespace should have
        # unique ports.
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind(('127.0.0.1', 0))
        self.port = self._sock.getsockname()[1]

    def teardown(self):
        self._sock.close()

    def test_no_stop(self):
        """Deleting a session without stopping it must tidy up"""
        # Filename is irrelevant, since it will never be opened
        config = _bf_ingest_session.SessionConfig('/not_a_real_filename')
        config.add_endpoint('239.1.2.3', self.port)
        session = _bf_ingest_session.Session(config)


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
        self.args = argparse.Namespace(
            cbf_spead=[endpoint.Endpoint('239.1.2.3', self.port)],
            file_base=self.tmpdir,
            direct_io=False,
            ibv=False,
            stream_name=None, spead_metadata=True,
            affinity=None, interface=None, telstate=None)
        self.loop = trollius.get_event_loop()

    def teardown(self):
        shutil.rmtree(self.tmpdir)
        self._sock.close()

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
    def _test_stream(self, end, telstate_metadata):
        n_channels = 768
        spectra_per_heap = 256
        n_heaps = 25              # number of heaps in time
        n_spectra = spectra_per_heap * n_heaps
        n_bengs = 8
        ticks_between_spectra = 8192
        channels_per_heap = n_channels // n_bengs
        # Pick some heaps to drop, including an entire slice
        drop = np.zeros((n_bengs, n_heaps), np.bool_)
        drop[:, 4] = True
        drop[2, 9] = True
        drop[7, 24] = True

        if telstate_metadata:
            self.args.stream_name = 'corr.beam_0x'
            self.args.telstate = {
                'cbf_corr_beam_0x_n_chans': n_channels,
                'cbf_corr_beam_0x_n_chans_per_substream': channels_per_heap,
                'cbf_ticks_between_spectra': ticks_between_spectra
            }
            self.args.spead_metadata = False

        # Start up the server
        server = bf_ingest_server.CaptureServer(self.args, self.loop)
        filename = server.start_capture()
        time.sleep(0.1)
        # Send it a SPEAD stream
        config = spead2.send.StreamConfig(max_packet_size=4196, rate=1e9 / 8)
        flavour = spead2.Flavour(4, 64, 48, 0)
        stream = spead2.send.UdpStream(spead2.ThreadPool(), '239.1.2.3', self.port, config)
        ig = spead2.send.ItemGroup(flavour=flavour)
        ig.add_item(name='timestamp', id=0x1600,
                    description='Timestamp', shape=(), format=[('u', 48)])
        ig.add_item(name='frequency', id=0x4103,
                    description='The frequency channel of the data in this HEAP.', shape=(), format=[('u', 48)])
        ig.add_item(name='bf_raw',  id=0x5000,
                    description='Beamformer data', shape=(channels_per_heap, spectra_per_heap, 2), dtype=np.int8)
        if not telstate_metadata:
            ig.add_item(name='ticks_between_spectra', id=0x104A,
                        description='Number of sample ticks between spectra.',
                        shape=(), format=[('u', 48)], value=ticks_between_spectra)
            ig.add_item(name='n_chans', id=0x1009,
                        description='The total number of frequency channels present in any integration.',
                        shape=(), format=[('u', 48)], value=n_channels)
        stream.send_heap(ig.get_heap())
        stream.send_heap(ig.get_start())
        ts = 1234567890
        for i in range(n_heaps):
            data = np.zeros((n_channels, spectra_per_heap, 2), np.int8)
            for channel in range(n_channels):
                data[channel, :, 0] = channel % 255 - 128
            for t in range(spectra_per_heap):
                data[:, t, 1] = (i * spectra_per_heap + t) % 255 - 128
            for j in range(n_bengs):
                ig['timestamp'].value = ts
                ig['frequency'].value = j * channels_per_heap
                ig['bf_raw'].value = data[j * channels_per_heap : (j + 1) * channels_per_heap, ...]
                if not drop[j, i]:
                    stream.send_heap(ig.get_heap())
            ts += spectra_per_heap * ticks_between_spectra
        if end:
            stream.send_heap(ig.get_end())
        stream = None

        # Wait for stream to shut down on its own
        yield From(trollius.sleep(0.1))
        # Shut down the session
        yield From(server.stop_capture())

        # Validate the output
        h5file = h5py.File(filename, 'r')
        with contextlib.closing(h5file):
            bf_raw = h5file['/Data/bf_raw']
            expected = np.zeros((n_channels, n_spectra, 2), np.int8)
            for channel in range(n_channels):
                expected[channel, :, 0] = channel % 255 - 128
            for t in range(n_spectra):
                expected[:, t, 1] = t % 255 - 128
            for i in range(n_bengs):
                for j in range(n_heaps):
                    if drop[i, j]:
                        channel0 = i * channels_per_heap
                        spectrum0 = j * spectra_per_heap
                        expected[channel0 : channel0 + channels_per_heap,
                                 spectrum0 : spectrum0 + spectra_per_heap, :] = 0
            np.testing.assert_equal(expected, bf_raw)

            timestamps = h5file['/Data/timestamps']
            expected = 1234567890 + ticks_between_spectra * np.arange(spectra_per_heap * n_heaps)
            np.testing.assert_equal(expected, timestamps)

            captured_timestamps = h5file['/Data/captured_timestamps']
            slice_drops = np.sum(drop, axis=0)
            captured_slices = np.nonzero(slice_drops == 0)[0]
            captured_spectra = captured_slices[:, np.newaxis] * spectra_per_heap + \
                    np.arange(spectra_per_heap)[np.newaxis, :]
            expected = 1234567890 + ticks_between_spectra * captured_spectra.flatten()
            np.testing.assert_equal(expected, captured_timestamps)

            flags = h5file['/Data/flags']
            expected = np.where(drop, 8, 0).astype(np.uint8)
            np.testing.assert_equal(expected, flags)

    def test_stream_end(self):
        """Stream ends with an end-of-stream"""
        trollius.get_event_loop().run_until_complete(self._test_stream(True, False))

    def test_stream_no_end(self):
        """Stream ends with a stop request"""
        trollius.get_event_loop().run_until_complete(self._test_stream(False, False))

    def test_stream_telstate_metadata(self):
        """Stream with metadata passed through telstate"""
        trollius.get_event_loop().run_until_complete(self._test_stream(True, True))
