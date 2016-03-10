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
from katsdpingest.bf_ingest_server import KatcpCaptureServer


@trollius.coroutine
def on_shutdown(server):
    logging.info('Shutting down')
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
