from Queue import LifoQueue
from katana.task import Task
import logging
logger = logging.getLogger(__name__)
from traceback import format_exc


class AbstractWorker(object):
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage

    def process_transaction(self, transaction_id):
        stack = LifoQueue()
        tasks = self.storage.get_tasks(transaction_id)
        logger.debug(tasks)
        for i, task in enumerate(tasks):
            try:
                task = Task(task)
                task.run()
                self.storage.set_task_processed(transaction_id, i, True)
                stack.put(task)
            except:
                logger.critical(format_exc())
                self.storage.set_task_processed(transaction_id, i, False)
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
