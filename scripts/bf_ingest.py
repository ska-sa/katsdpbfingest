#!/usr/bin/env python3

import signal
import asyncio
import logging
import os
import sys

import katsdpservices

from katsdpbfingest.bf_ingest_server import KatcpCaptureServer, parse_args


def on_shutdown(server: KatcpCaptureServer) -> None:
    logging.info('Shutting down')
    loop = asyncio.get_event_loop()
    loop.remove_signal_handler(signal.SIGINT)
    loop.remove_signal_handler(signal.SIGTERM)
    server.halt()


async def main() -> None:
    katsdpservices.setup_logging()
    katsdpservices.setup_restart()

    args = parse_args()
    if args.log_level is not None:
        logging.root.setLevel(args.log_level.upper())
    if args.file_base is None and args.stats is None:
        logging.warning('Neither --file-base nor --stats was given; nothing useful will happen')
    if args.file_base is not None and not os.access(args.file_base, os.W_OK):
        logging.error('Target directory (%s) is not writable', args.file_base)
        sys.exit(1)

    loop = asyncio.get_event_loop()
    server = KatcpCaptureServer(args, loop)
    loop.add_signal_handler(signal.SIGINT, lambda: on_shutdown(server))
    loop.add_signal_handler(signal.SIGTERM, lambda: on_shutdown(server))
    await server.start()
    await server.join()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
