from unittest import TestCase
import socket
from json import loads, dumps
from uuid import UUID

INVALID_OPERATION = {
    'status': 'error',
    'message': 'Invalid operation'
}

class CommunicationTestCase(TestCase):
    def send(self, message):
        s = socket.socket(socket.AF_UNIX)
        s.connect('katana.sock')
        s.sendall(dumps(message) + '\n')
        message = loads(s.recv(4096))
        s.close()
        if message['status'] == 'error':
            raise Exception(message['message'])
        return message['result']

    def test_that_passing_invalid_json_results_in_error(self):
        self.assertRaises(Exception, self.send)

    def test_that_passing_invalid_operation_results_in_error(self):
        self.assertRaises(Exception, self.send, {'action': 'inexisting'})

    def test_that_initiated_transaction_has_a_valid_uuid(self):
        transaction = self.send({
            'action': 'begin_transaction',
        })
        self.assertIsNotNone(transaction)
        x = UUID(transaction)
        self.assertEquals(x.hex, transaction)