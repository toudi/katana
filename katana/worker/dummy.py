from abstract import AbstractWorker
from katana.runner._socket import get_server
from katana.runner._socket import get_client
from katana.runner._socket import recv_all
from json import loads
from json import dumps
import SocketServer
import logging

logger = logging.getLogger(__name__)


class Handler(SocketServer.StreamRequestHandler):
    def handle(self):
        transaction_id = self.rfile.readline().strip()
        logging.debug('Processing %s' % transaction_id)
        result = self.server.worker.process_transaction(transaction_id)
        self.request.sendall(dumps(result))


class Worker(AbstractWorker):
    def run(self):
        server = get_server(self.config, 'dummy_worker', Handler)
        server.worker = self
        server.serve_forever()


class Client(object):
    def __init__(self, config):
        self.config = config

    def process_transaction(self, transaction_id, background=True):
        # this is a dummy client, so we're going to ignore the background
        # parameter
        client = get_client(self.config, 'dummy_worker')
        logging.debug('Send %r to %r' % (transaction_id, client))
        client.sendall('%s\n' % transaction_id)
        response = loads(recv_all(client))
        return response
