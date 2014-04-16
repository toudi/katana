from Queue import LifoQueue
from importlib import import_module
from katana.task import Task


class AbstractWorker(object):
    def __init__(self, config):
        self.config = config
        self._client = None

    def process_transaction(self, transaction_id):
        stack = LifoQueue()
        tasks = self.client.get_tasks(transaction_id)
        for i, task in enumerate(tasks):
            try:
                task = Task(task)
                task.run()
                self.client.set_task_processed(transaction_id, i, True)
                stack.put(task)
            except:
                self.client.set_task_processed(transaction_id, i, False)
                while stack.qsize():
                    task = stack.get()
                    task.reverse()
                return {
                    'error': True,
                    'processed': i,
                }
        return {
            'success': True
        }


    @property
    def client(self):
        if not self._client:
            self._client = import_module(
                'katana.runner.%s' % self.config.get('katana', 'runner')
            ).Client(self.config)
        return self._client