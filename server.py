import SocketServer
import argparse
from ConfigParser import SafeConfigParser
from handler import KatanaHandler
from service import KatanaService

parser = argparse.ArgumentParser(description='Distributed transaction manager')
parser.add_argument('--host', help='A host to bind to')
parser.add_argument('--port', help='A port to bind to')
parser.add_argument('--socket', help='A unix socket to bind to', default='katana.sock')

args = parser.parse_args()

SocketServer.TCPServer.allow_reuse_address = True
if args.socket:
    server = SocketServer.ThreadingUnixStreamServer(args.socket, KatanaHandler)
else:
    server = SocketServer.ThreadingTCPServer((args.host, args.port), KatanaHandler)
server.katana = KatanaService()
server.serve_forever()
server.shutdown()