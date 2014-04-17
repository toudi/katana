import SocketServer
import socket
from json import loads, dumps
import os
import logging

logger = logging.getLogger(__name__)


def get_server(config, config_section, handler):
    SocketServer.TCPServer.allow_reuse_address = True

    if config.has_option(config_section, 'socket'):
        socket_file = config.get(config_section, 'socket')
        if os.path.exists(socket_file):
            os.unlink(socket_file)
        server = SocketServer.ThreadingUnixStreamServer(
            socket_file,
            handler
        )
    else:
        server = SocketServer.ThreadingTCPServer(
            (
                config.get(config_section, 'host'),
                config.getint(config_section, 'port')
            ), handler
        )
    return server


def get_client(config, config_section):
    if config.has_option(config_section, 'socket'):
        _socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        _socket.connect(config.get(config_section, 'socket'))
    else:
        _socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        _socket.connect((config.get(config_section, 'host'), config.getint(config_section, 'port')))
    return _socket


def recv_all(_socket):
    total_data=[]
    while True:
        data = _socket.recv(8192)
        if not data: break
        total_data.append(data)
    _socket.close()
    return ''.join(total_data)

class Runner(object):
    def __init__(self, katana):
        server = get_server(katana.config, 'socket', Handler)
        server.katana = katana
        self.server = server

    def run(self):
        self.server.serve_forever()

class Handler(SocketServer.StreamRequestHandler):
    def handle(self):
        try:
            try:
                data = loads(self.rfile.readline().strip())
                logger.debug(data)
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
        return get_client(self.config, 'socket')

    def _send(self, data):
        _socket = self.get_socket()
        _socket.sendall(dumps(data)+'\n')
        response = loads(_socket.recv(1024))
        _socket.close()

        try:
            return response['result']
        except KeyError:
            raise Exception(response['message'])