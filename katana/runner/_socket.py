import SocketServer
import socket
from json import loads, dumps
import os


class Runner(object):
    def __init__(self, katana):
        SocketServer.TCPServer.allow_reuse_address = True

        if katana.config.has_option('socket', 'socket'):
            socket_file = katana.config.get('socket', 'socket')
            if os.path.exists(socket_file):
                os.unlink(socket_file)
            server = SocketServer.ThreadingUnixStreamServer(
                socket_file,
                Handler
            )
        else:
            server = SocketServer.ThreadingTCPServer(
                (
                    katana.config.get('socket', 'host'),
                    katana.config.getint('socket', 'port')
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


class Client(object):
    def __init__(self, config):
        self.config = config

    def begin_transaction(self):
        return self._send({
            'action': 'begin_transaction'
        })

    def add_operation(self, transaction_id, operation, reverse_operation, task_runner):
        return self._send({
            'action': 'add_operation',
            'args': [transaction_id, operation, reverse_operation, task_runner]
        })

    def commit(self, transaction_id, background=True):
        return self._send({
            'action': 'commit',
            'args': [transaction_id, background]
        })

    def get_tasks(self, transaction_id):
        return self._send({
            'action': 'get_tasks',
            'kwargs': {'transaction_id': transaction_id}
        })

    def set_task_processed(self, transaction_id, task, processed):
        return self._send({
            'action': 'set_task_processed',
            'kwargs': {
                'transaction_id': transaction_id,
                'operation': task,
                'is_processed': processed
            }
        })

    def get_socket(self):
        if self.config.has_option('socket', 'socket'):
            _socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            _socket.connect(self.config.get('socket', 'socket'))
        else:
            _socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            _socket.connect((self.config.get('socket', 'host'), self.config.getint('socket', 'port')))
        return _socket

    def _send(self, data):
        _socket = self.get_socket()
        _socket.sendall(dumps(data)+'\n')
        response = loads(_socket.recv(1024))
        _socket.close()

        try:
            return response['result']
        except KeyError:
            raise Exception(response['message'])