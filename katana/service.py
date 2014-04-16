from uuid import uuid4
from importlib import import_module
from ConfigParser import SafeConfigParser
from importlib import import_module
import logging
logger = logging.getLogger(__name__)
import sys


class KatanaService(object):
    def __init__(self, args):
        """
        @type config: argparse.Namespace
        """
        logger.debug('using %s as config file' % args['config'])
        self.config = SafeConfigParser()
        self.config.read(args['config'])

        if self.config.has_option('katana', 'paths'):
            for path in self.config.get('katana', 'paths').split(','):
                logging.debug('appending %s to PYTHONPATH' % path)
                sys.path.append(path)

        # whether to launch a built-in server
        if self.config.has_option('katana', 'runner'):
            runner_module = import_module('katana.runner.%s' % self.config.get('katana', 'runner'))
            self.runner = runner_module.Runner(self)

        if not self.config.has_option('katana', 'storage'):
            raise Exception('storage not defined')

        if not self.config.has_option('katana', 'worker'):
            raise Exception('worker not defined')

        self.storage = import_module(self.config.get('katana', 'storage')).Storage(
            config=self.config
        )

        self.worker = import_module(self.config.get('katana', 'worker')).Client(
            config=self.config
        )

    def begin_transaction(self):
        id = uuid4().hex
        self.storage.create(id)
        return id

    def add_operation(self, transaction, operation, reverse_operation, task_runner):
        """
        Every operation must have it's corresponding reversing operation,
        which will be executed in case of any error/exception.
        The operation must be serialized as string, and have it's implementation
        (which would be project-specific code)
        a task runner is a form of client which takes operation as it's input, and runs
        the operation in question.
        """
        if not reverse_operation:
            raise Exception("reverse operation not provided")
        self.storage.add_operation(transaction, operation, reverse_operation, task_runner)

    def commit(self, transaction_id, background=True):
        result = self.worker.process_transaction(transaction_id, background)
        if not background:
            return result

    def run(self):
        self.runner.run()

    # client-related methods
    def get_tasks(self, transaction_id):
        return self.storage.get_tasks(transaction_id)

    def set_task_processed(self, transaction_id, operation, is_processed):
        self.storage.set_task_processed(transaction_id, operation, is_processed)