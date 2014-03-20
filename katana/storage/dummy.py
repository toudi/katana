"""
This is a dummy, in-memory storage.
It is merely a proof of concept and it is meant for
development purposes.
"""
from katana.task import Task


class Storage(object):
    def __init__(self):
        self.transactions = {}

    def create(self, transaction):
        self.transactions[transaction] = []

    def add_operation(self, transaction, operation, reverse_operation, task_runner):
        self.transactions[transaction].append({
            'operation': operation,
            'reverse_operation': reverse_operation,
            'task_runner': task_runner
        })

    def set_task_processed(self, transaction, operation, is_processed):
        self.transactions[transaction]['is_processed'] = is_processed

    def get_tasks(self, transaction_id):
        for task in self.transactions[transaction]:
            yield