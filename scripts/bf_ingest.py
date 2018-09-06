#!/usr/bin/env python

from __future__ import print_function, division, absolute_import
import signal
import trollius
from tornado.platform.asyncio import AsyncIOMainLoop, to_asyncio_future
import logging
from trollius import From
import argparse
import os
import sys
import katsdptelstate.endpoint
import spead2
import katsdpservices
from katsdpbfingest.bf_ingest_server import KatcpCaptureServer
from katsdpbfingest.utils import Range


@trollius.coroutine
def on_shutdown(server):
    logging.info('Shutting down')
    trollius.get_event_loop().remove_signal_handler(signal.SIGINT)
    trollius.get_event_loop().remove_signal_handler(signal.SIGTERM)
    yield From(to_asyncio_future(server.stop()))
    trollius.get_event_loop().stop()


def main():
    katsdpservices.setup_logging()
    katsdpservices.setup_restart()
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
        '--file-base', default='.', type=str, metavar='DIR',
        help='base directory into which to write HDF5 files')
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
        '--stats', type=katsdptelstate.endpoint.endpoint_parser(7149), metavar='ENDPOINT',
        help='Send statistics to a signal display server at this address')
    parser.add_argument(
        '--stats-int-time', type=float, default=1.0, metavar='SECONDS',
        help='Interval between sending statistics to the signal displays')
    parser.add_argument(
        '--stats-interface', type=str,
        help='Network interface for signal display stream')
    parser.add_argument('--port', '-p', type=int, default=2050, help='katcp host port')
    parser.add_argument('--host', '-a', type=str, default='', help='katcp host address')
    args = parser.parse_args()
    if args.affinity and len(args.affinity) < 2:
        parser.error('At least 2 CPUs must be specified for --affinity')
    if args.telstate is None:
        parser.error('--telstate is required')
    if args.stream_name is None:
        parser.error('--stream-name is required')
    if args.log_level is not None:
        logging.root.setLevel(args.log_level.upper())
    if not os.access(args.file_base, os.W_OK):
        logging.error('Target directory (%s) is not writable', args.file_base)
        sys.exit(1)
    ioloop = AsyncIOMainLoop()
    ioloop.install()
    server = KatcpCaptureServer(args, trollius.get_event_loop())
    server.set_concurrency_options(thread_safe=False, handler_thread=False)
    server.set_ioloop(ioloop)
    trollius.get_event_loop().add_signal_handler(
        signal.SIGINT, lambda: trollius.async(on_shutdown(server)))
    trollius.get_event_loop().add_signal_handler(
        signal.SIGTERM, lambda: trollius.async(on_shutdown(server)))
    ioloop.add_callback(server.start)
    trollius.get_event_loop().run_forever()


if __name__ == '__main__':
    main()
