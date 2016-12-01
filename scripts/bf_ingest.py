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
import time
import katsdptelstate.endpoint
import spead2
from katsdpingest.bf_ingest_server import KatcpCaptureServer


@trollius.coroutine
def on_shutdown(server):
    logging.info('Shutting down')
    trollius.get_event_loop().remove_signal_handler(signal.SIGINT)
    trollius.get_event_loop().remove_signal_handler(signal.SIGTERM)
    yield From(to_asyncio_future(server.stop()))
    trollius.get_event_loop().stop()


def configure_logging(level):
    formatter = logging.Formatter("%(asctime)s.%(msecs)03dZ - %(filename)s:%(lineno)s - %(levelname)s - %(message)s",
                                  datefmt="%Y-%m-%d %H:%M:%S")
    formatter.converter = time.gmtime
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logging.root.addHandler(sh)
    logging.root.setLevel(level.upper())


def main():
    parser = katsdptelstate.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--cbf-channels', type=int, help='unused, kept for backwards compatibility')
    parser.add_argument('--cbf-spead', type=katsdptelstate.endpoint.endpoint_list_parser(7148), default=':7148', help='endpoints to listen for CBF SPEAD stream (including multicast IPs). [<ip>[+<count>]][:port].', metavar='ENDPOINTS')
    parser.add_argument('--no-spead-metadata', dest='spead_metadata', default=True, action='store_false', help='Ignore metadata sent in SPEAD stream')
    parser.add_argument('--stream-name', type=str, metavar='NAME', help='Stream name for metadata in telstate')
    parser.add_argument('--log-level', '-l', type=str, metavar='LEVEL', default='INFO', help='log level')
    parser.add_argument('--file-base', default='.', type=str, help='base directory into which to write HDF5 files', metavar='DIR')
    parser.add_argument('--affinity', type=spead2.parse_range_list, help='List of CPUs to which to bind threads', metavar='CPU,CPU')
    parser.add_argument('--interface', type=str, help='Network interface for multicast subscription')
    parser.add_argument('--direct-io', action='store_true', help='Use Direct I/O VFD for writing the file')
    parser.add_argument('--ibv', action='store_true', help='Use libibverbs when possible')
    parser.add_argument('--port', '-p', type=int, default=2050, help='katcp host port')
    parser.add_argument('--host', '-a', type=str, default='', help='katcp host address')
    args = parser.parse_args()
    if args.affinity and len(args.affinity) < 2:
        parser.error('At least 2 CPUs must be specified for --affinity')
    configure_logging(args.log_level)
    if not os.access(args.file_base, os.W_OK):
        logging.error('Target directory (%s) is not writable', args.file_base)
        sys.exit(1)
    if args.cbf_channels is not None:
        logging.warn('Option --cbf-channels is unused and will be removed in future')
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
