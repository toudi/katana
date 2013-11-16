"""
This is a dummy, in-memory storage.
It is merely a proof of concept and it is meant for
development purposes.
"""
class Storage(object):
    def __init__(self):
        self.transactions = {}

    def create(self, transaction):
        self.transactions[transaction] = []

    def add_operation(self, transaction, operation, reverse_operation):
        pass