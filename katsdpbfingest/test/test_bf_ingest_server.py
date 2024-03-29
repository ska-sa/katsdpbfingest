"""Tests for the bf_ingest_server module"""

import argparse
import tempfile
import shutil
import os.path
import contextlib
import socket
import asyncio
from unittest import mock
from typing import List, Optional

import h5py
import numpy as np

import spead2
import spead2.recv
import spead2.send

import asynctest
from nose.tools import assert_equal, assert_true, assert_false, assert_is_none

import katsdptelstate
from katsdptelstate import endpoint

from katsdpbfingest import bf_ingest_server, _bf_ingest


DATA_LOST = 1 << 3


class TestSession:
    def setup(self) -> None:
        # To avoid collisions when running tests in parallel on a single host,
        # create a socket for the duration of the test and use its port as the
        # port for the test. Sockets in the same network namespace should have
        # unique ports.
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind(('127.0.0.1', 0))
        self.port = self._sock.getsockname()[1]
        self.tmpdir = tempfile.mkdtemp()

    def teardown(self) -> None:
        shutil.rmtree(self.tmpdir)
        self._sock.close()

    def test_no_stop(self) -> None:
        """Deleting a session without stopping it must tidy up"""
        config = _bf_ingest.SessionConfig(os.path.join(self.tmpdir, 'test_no_stop.h5'))
        config.add_endpoint('239.1.2.3', self.port)
        config.channels = 4096
        config.channels_per_heap = 256
        config.spectra_per_heap = 256
        config.ticks_between_spectra = 8192
        config.sync_time = 1111111111.0
        config.bandwidth = 856e6
        config.center_freq = 1284e6
        config.scale_factor_timestamp = 1712e6
        config.heaps_per_slice_time = 2
        _bf_ingest.Session(config)


def _make_listen_socket():
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    sock.listen()
    return sock


class TestCaptureServer(asynctest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmpdir)
        self.port = 7148
        self.n_channels = 1024
        self.spectra_per_heap = 256
        # No data actually travels through these multicast groups;
        # it gets mocked out to use local TCP sockets instead.
        self.endpoints = endpoint.endpoint_list_parser(self.port)(
            '239.102.2.0+7:{}'.format(self.port))
        self.tcp_acceptors = [_make_listen_socket() for endpoint in self.endpoints]
        self.tcp_endpoints = [endpoint.Endpoint(*sock.getsockname()) for sock in self.tcp_acceptors]
        self.n_bengs = 16
        self.ticks_between_spectra = 8192
        self.adc_sample_rate = 1712000000.0
        self.heaps_per_stats = 6
        self.channels_per_heap = self.n_channels // self.n_bengs
        self.channels_per_endpoint = self.n_channels // len(self.endpoints)
        attrs = {
            'i0_tied_array_channelised_voltage_0x_n_chans': self.n_channels,
            'i0_tied_array_channelised_voltage_0x_n_chans_per_substream': self.channels_per_heap,
            'i0_tied_array_channelised_voltage_0x_spectra_per_heap': self.spectra_per_heap,
            'i0_tied_array_channelised_voltage_0x_src_streams': [
                'i0_antenna_channelised_voltage'],
            'i0_tied_array_channelised_voltage_0x_bandwidth': self.adc_sample_rate / 2,
            'i0_tied_array_channelised_voltage_0x_center_freq': 3 * self.adc_sample_rate / 2,
            'i0_antenna_channelised_voltage_ticks_between_spectra': self.ticks_between_spectra,
            'i0_antenna_channelised_voltage_instrument_dev_name': 'i0',
            'i0_sync_time': 111111111.0,
            'i0_scale_factor_timestamp': self.adc_sample_rate
        }
        telstate = katsdptelstate.TelescopeState()
        for key, value in attrs.items():
            telstate[key] = value
        stats_int_time = (self.heaps_per_stats * self.ticks_between_spectra *
                          self.spectra_per_heap / self.adc_sample_rate)
        self.args = bf_ingest_server.parse_args([
            '--cbf-spead=' + endpoint.endpoints_to_str(self.endpoints),
            '--channels=128:768',
            '--file-base=' + self.tmpdir,
            '--stream-name=i0_tied_array_channelised_voltage_0x',
            '--interface=lo',
            '--stats=239.102.3.0:7149',
            '--stats-int-time={}'.format(stats_int_time),
            '--stats-interface=lo'],
            argparse.Namespace(telstate=telstate))
        self.loop = asyncio.get_event_loop()
        self.patch_add_endpoint()
        self.patch_create_session_config()
        self.patch_session_factory()

    def patch_add_endpoint(self):
        """Prevent actual endpoints from being added, since we're using TCP instead."""
        patcher = mock.patch.object(_bf_ingest.SessionConfig, 'add_endpoint')
        patcher.start()
        self.addCleanup(patcher.stop)

    def patch_create_session_config(self):
        """Force heaps_per_slice_time to 2.

        The test is written around this value, but the default is to compute
        it from other parameters.
        """
        def create_session_config(args: argparse.Namespace) -> _bf_ingest.SessionConfig:
            config = orig_create_session_config(args)
            config.heaps_per_slice_time = 2
            return config

        orig_create_session_config = bf_ingest_server.create_session_config
        patcher = mock.patch.object(
            bf_ingest_server, 'create_session_config', create_session_config)
        patcher.start()
        self.addCleanup(patcher.stop)

    def patch_session_factory(self):
        def session_factory(config: _bf_ingest.SessionConfig) -> _bf_ingest.Session:
            session = _bf_ingest.Session(config)
            for sock in self.tcp_acceptors:
                session.add_tcp_reader(sock)
                sock.close()
            return session

        patcher = mock.patch.object(bf_ingest_server, 'session_factory', session_factory)
        patcher.start()
        self.addCleanup(patcher.stop)

    async def test_manual_stop_no_data(self) -> None:
        """Manual stop before any data is received"""
        server = bf_ingest_server.CaptureServer(self.args, self.loop)
        assert_false(server.capturing)
        await server.start_capture('1122334455')
        assert_true(server.capturing)
        await asyncio.sleep(0.01)
        await server.stop_capture()
        assert_false(server.capturing)

    async def _test_stream(self, end: bool, write: bool) -> None:
        n_heaps = 30              # number of heaps in time
        n_spectra = self.spectra_per_heap * n_heaps
        # Pick some heaps to drop, including an entire slice and
        # an entire channel for one stats dump
        drop = np.zeros((self.n_bengs, n_heaps), np.bool_)
        drop[:, 4] = True
        drop[2, 9] = True
        drop[7, 24] = True
        drop[10, 12:18] = True
        if not write:
            self.args.file_base = None

        # Start a receiver to get the signal display stream.
        # It needs a deep queue because we don't service it while it is
        # running.
        rx = spead2.recv.Stream(
            spead2.ThreadPool(),
            spead2.recv.StreamConfig(max_heaps=2, stop_on_stop_item=False),
            spead2.recv.RingStreamConfig(heaps=100)
        )
        rx.add_udp_reader(self.args.stats.host, self.args.stats.port,
                          interface_address='127.0.0.1')

        # Start up the server
        server = bf_ingest_server.CaptureServer(self.args, self.loop)
        filename = await server.start_capture('1122334455')
        # Send it a SPEAD stream. Use small packets to ensure that each heap is
        # split into multiple packets, to check that the data scatter works.
        config = spead2.send.StreamConfig(max_packet_size=256)
        flavour = spead2.Flavour(4, 64, 48, 0)
        ig = spead2.send.ItemGroup(flavour=flavour)
        ig.add_item(name='timestamp', id=0x1600,
                    description='Timestamp', shape=(), format=[('u', 48)])
        ig.add_item(name='frequency', id=0x4103,
                    description='The frequency channel of the data in this HEAP.',
                    shape=(), format=[('u', 48)])
        ig.add_item(name='bf_raw', id=0x5000,
                    description='Beamformer data',
                    shape=(self.channels_per_heap, self.spectra_per_heap, 2),
                    dtype=np.dtype(np.int8))
        # To guarantee in-order delivery (and hence make the test
        # reliable/reproducible), we send all the data for the channels of
        # interest through a single TCP socket. Data for channels outside the
        # subscribed range is discarded. Note that giving multiple TcpStream's
        # the same socket is dangerous because individual write() calls could
        # interleave; it's safe only because we use only blocking calls so
        # there is no concurrency between the streams.
        subscribed_streams = self.args.channels // self.channels_per_endpoint
        subscribed_bengs = self.args.channels // self.channels_per_heap
        expected_heaps = 0
        streams = []    # type: List[Optional[spead2.send.TcpStream]]
        primary_ep = self.tcp_endpoints[subscribed_streams.start]
        sock = socket.socket()
        sock.setblocking(False)
        await self.loop.sock_connect(sock, (primary_ep.host, primary_ep.port))
        for i in range(len(self.endpoints)):
            if i not in subscribed_streams:
                streams.append(None)
                continue
            stream = spead2.send.TcpStream(spead2.ThreadPool(), sock, config)
            streams.append(stream)
            stream.set_cnt_sequence(i, len(self.endpoints))
            stream.send_heap(ig.get_heap(descriptors='all'))
            stream.send_heap(ig.get_start())
            expected_heaps += 2
        sock.close()
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
                    heap = ig.get_heap()
                    # The receiver looks at inline items in each packet to place
                    # data correctly.
                    heap.repeat_pointers = True
                    if j in subscribed_bengs:
                        out_stream = streams[j // (self.n_bengs // len(self.endpoints))]
                        assert out_stream is not None
                        out_stream.send_heap(heap)
                        expected_heaps += 1
            ts += self.spectra_per_heap * self.ticks_between_spectra
        if end:
            for out_stream in streams:
                if out_stream is not None:
                    out_stream.send_heap(ig.get_end())
                    # They're all pointing at the same receiver, which will
                    # shut down after the first stop heap
                    break
        streams = []

        if not end:
            # We only want to stop the capture once all the heaps we expect
            # have been received, but time out after 5 seconds to avoid
            # hanging the test.
            for i in range(100):
                if server.counters['heaps'] >= expected_heaps:
                    break
                else:
                    print('Only {} / {} heaps received so far'.format(
                          server.counters['heaps'], expected_heaps))
                    await asyncio.sleep(0.05)
            else:
                print('Giving up waiting for heaps')
        await server.stop_capture(force=not end)

        expected_data = np.zeros((self.n_channels, n_spectra, 2), np.int8)
        expected_weight = np.ones((self.n_channels, n_spectra), np.int8)
        for channel in range(self.n_channels):
            expected_data[channel, :, 0] = channel % 255 - 128
        for t in range(n_spectra):
            expected_data[:, t, 1] = t % 255 - 128
        for i in range(self.n_bengs):
            for j in range(n_heaps):
                if drop[i, j]:
                    channel0 = i * self.channels_per_heap
                    spectrum0 = j * self.spectra_per_heap
                    index = np.s_[channel0 : channel0 + self.channels_per_heap,
                                  spectrum0 : spectrum0 + self.spectra_per_heap]
                    expected_data[index] = 0
                    expected_weight[index] = 0
        expected_data = expected_data[self.args.channels.asslice()]
        expected_weight = expected_weight[self.args.channels.asslice()]

        # Validate the output
        if write:
            h5file = h5py.File(filename, 'r')
            with contextlib.closing(h5file):
                bf_raw = h5file['/Data/bf_raw']
                np.testing.assert_equal(expected_data, bf_raw)

                timestamps = h5file['/Data/timestamps']
                expected = 1234567890 \
                    + self.ticks_between_spectra * np.arange(self.spectra_per_heap * n_heaps)
                np.testing.assert_equal(expected, timestamps)

                flags = h5file['/Data/flags']
                expected = np.where(drop, 8, 0).astype(np.uint8)
                expected = expected[self.args.channels.start // self.channels_per_heap :
                                    self.args.channels.stop // self.channels_per_heap]
                np.testing.assert_equal(expected, flags)

                data_set = h5file['/Data']
                assert_equal('i0_tied_array_channelised_voltage_0x', data_set.attrs['stream_name'])
                assert_equal(self.args.channels.start, data_set.attrs['channel_offset'])
        else:
            assert_is_none(filename)

        # Validate the signal display stream
        rx.stop()
        heaps = list(rx)
        # Note: would need updating if n_heaps is not a multiple of heaps_per_stats
        assert_equal(n_heaps // self.heaps_per_stats + 2, len(heaps))
        assert_true(heaps[0].is_start_of_stream())
        assert_true(heaps[-1].is_end_of_stream())
        ig = spead2.send.ItemGroup()
        spectrum = 0
        spectra_per_stats = self.heaps_per_stats * self.spectra_per_heap
        for rx_heap in heaps[1:-1]:
            updated = ig.update(rx_heap)
            rx_data = updated['sd_data'].value
            rx_flags = updated['sd_flags'].value
            rx_timestamp = updated['sd_timestamp'].value

            # Check types and shapes
            assert_equal((len(self.args.channels), 2, 2), rx_data.shape)
            assert_equal(np.float32, rx_data.dtype)
            assert_equal((len(self.args.channels), 2), rx_flags.shape)
            assert_equal(np.uint8, rx_flags.dtype)
            np.testing.assert_equal(0, rx_data[..., 1])  # Should be real only

            rx_power = rx_data[:, 0, 0]
            rx_saturated = rx_data[:, 1, 0]

            # Check calculations
            ts_unix = (spectrum + 0.5 * spectra_per_stats) * self.ticks_between_spectra \
                / self.adc_sample_rate + 111111111.0
            np.testing.assert_allclose(ts_unix * 100.0, rx_timestamp)

            index = np.s_[:, spectrum : spectrum + spectra_per_stats]
            frame_data = expected_data[index]
            frame_weight = expected_weight[index]
            weight_sum = np.sum(frame_weight, axis=1)
            power = np.sum(frame_data.astype(np.float64)**2, axis=2)    # Sum real+imag
            saturated = (frame_data == -128) | (frame_data == 127)
            saturated = np.logical_or.reduce(saturated, axis=2)         # Combine real+imag
            saturated = saturated.astype(np.float64)
            # Average over time. Can't use np.average because it complains if
            # weights sum to zero instead of giving a NaN.
            with np.errstate(divide='ignore', invalid='ignore'):
                power = np.sum(power * frame_weight, axis=1) / weight_sum
                saturated = np.sum(saturated * frame_weight, axis=1) / weight_sum
            power = np.where(weight_sum, power, 0)
            saturated = np.where(weight_sum, saturated, 0)
            np.testing.assert_allclose(power, rx_power)
            np.testing.assert_allclose(saturated, rx_saturated)
            flags = np.where(weight_sum, 0, DATA_LOST)
            np.testing.assert_equal(flags, rx_flags[:, 0])

            spectrum += spectra_per_stats

    async def test_stream_end(self) -> None:
        """Stream ends with an end-of-stream"""
        await self._test_stream(True, True)

    async def test_stream_no_end(self) -> None:
        """Stream ends with a stop request"""
        await self._test_stream(False, True)

    async def test_stream_no_write(self) -> None:
        """Stream with only statistics, no output file"""
        await self._test_stream(True, False)
