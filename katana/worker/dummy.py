from abstract import AbstractWorker


class Worker(AbstractWorker):
    pass


class Client(object):
    def __init__(self, config):
        self.config = config
        self.worker = Worker(config)

    def process_transaction(self, transaction_id, background=True):
        # this is a dummy client, so we're going to ignore the background
        # parameter
        return self.worker.process_transaction(transaction_id)

