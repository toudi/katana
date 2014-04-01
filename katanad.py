#!/bin/env python2
import argparse
from katana.service import KatanaService
import logging
logging.basicConfig(level=logging.DEBUG)

parser = argparse.ArgumentParser(description='Distributed transaction manager')
parser.add_argument('--config', default='katana.ini', help='Config file')
parser.add_argument('--syncdb', action='store_true', help='Synchronize the database')

args = parser.parse_args()

katana = KatanaService(args.__dict__)
katana.run()

# SocketServer.TCPServer.allow_reuse_address = True
# if args.socket:
#     server = SocketServer.ThreadingUnixStreamServer(args.socket, KatanaHandler)
# else:
#     server = SocketServer.ThreadingTCPServer(
#         (args.host, args.port), KatanaHandler
#     )
# server.katana = KatanaService()
# server.serve_forever()
# server.shutdown()
