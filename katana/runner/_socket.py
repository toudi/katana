import SocketServer
from json import loads, dumps
import signal
import os


class Runner(object):
    def __init__(self, katana):
        SocketServer.TCPServer.allow_reuse_address = True

        if katana.config.has_option('socket', 'socket'):
            socket_file = katana.config.get('socket', 'socket')
            if os.path.exists(socket_file):
                os.unlink(socket_file)
            server = SocketServer.UnixStreamServer(
                socket_file,
                Handler
            )
        else:
            server = SocketServer.TCPServer(
                (
                    katana.config.get('socket', 'host'),
                    katana.config.get('socket', 'port')
                ), Handler
            )

        server.katana = katana
        self.server = server

    def run(self):
        self.server.serve_forever()

class Handler(SocketServer.StreamRequestHandler):
    def handle(self):
        try:
            try:
                data = loads(self.rfile.readline().strip())
                method = getattr(self.server.katana, data['action'])
            except (ValueError, AttributeError, KeyError):
                raise Exception("Invalid operation")

            result = {
                'status': 'ok',
                'result': method(
                    *data.get('args', []),
                    **data.get('kwargs', {})
                )
            }

        except Exception, e:
            result = {
                'status': 'error',
                'message': unicode(e)
            }
        self.request.sendall(dumps(result))
