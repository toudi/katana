from gearman.client import GearmanClient
from gearman.worker import GearmanWorker
from abstract import AbstractWorker
from json import loads
from json import dumps


class Client(object):
    def __init__(self, config):
        self.config = config
        self.client = GearmanClient(
            self.config.get('gearman', 'hosts').split(',')
        )

    def process_transaction(self, transaction_id, background=True):
        job = self.client.submit_job(
            self.config.get('gearman', 'taskname'),
            str(transaction_id),
            background=background
        )
        if not background:
            return loads(job.result)

class Worker(AbstractWorker):
    def process(self, worker, job):
        result = self.process_transaction(unicode(job.data))
        return dumps(result)

    def run(self):
        self.worker = GearmanWorker(
            self.config.get('gearman', 'hosts').split(',')
        )
        self.worker.register_task(
            self.config.get('gearman', 'taskname'),
            self.process
        )
        self.worker.work()