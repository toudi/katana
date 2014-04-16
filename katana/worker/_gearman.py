from gearman.client import GearmanClient
from gearman.worker import GearmanWorker
from abstract import AbstractWorker


class Client(object):
    def __init__(self, config):
        self.config = config
        self.client = GearmanClient(
            self.config.get('gearman', 'hosts').split(',')
        )

    def process_transaction(self, transaction_id, background=True):
        self.client.submit_job(
            self.config.get('gearman', 'taskname'),
            str(transaction_id),
            background=background
        )

class Worker(AbstractWorker):
    def __init__(self, config):
        super(Worker, self).__init__(config)
        self.worker = GearmanWorker(
            self.config.get('gearman', 'hosts').split(',')
        )
        self.worker.register_task(self.config.get('gearman', 'taskname'), self.process)

    def process(self, worker, job):
        self.process_transaction(unicode(job.data))
        return 'ok'

    def run(self):
        self.worker.work()