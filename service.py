from uuid import uuid4


class KatanaService(object):
    def begin_transaction(self, parent_transaction=None):
        return uuid4().hex