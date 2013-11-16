from uuid import uuid4
from importlib import import_module


class KatanaService(object):
    def __init__(self):
        # TODO: some config file with storage cfg.
        self.storage = import_module('storage.dummy').Storage()

    def begin_transaction(self, parent_transaction=None):
        id = uuid4().hex
        self.storage.create(id)
        return id

    def add_operation(self, transaction, operation, reverse_operation):
        """
        Every operation must have it's corresponding reversing operation,
        which will be executed in case of any error/exception.
        The operation must be serialized as string, and have it's implementation
        (which would be project-specific code)
        """
        self.storage.add_operation(transaction, operation, reverse_operation)