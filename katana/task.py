from importlib import import_module


class Task(object):
    def __init__(self, serialized_task):
        self.operation = serialized_task['operation']
        self.reverse_operation = serialized_task['reverse_operation']
        self.task_runner = import_module(serialized_task['task_runner']).execute_task

    def run(self):
        self.task_runner(self.operation)

    def reverse(self):
        self.task_runner(self.reverse_operation)