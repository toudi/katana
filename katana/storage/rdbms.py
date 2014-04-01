from katana.task import Task
from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import Boolean
from sqlalchemy import Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Sequence
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Transaction(Base):
    __tablename__ = 'transaction'
    uuid = Column(String(length=32), primary_key=True)
    status = Column(Integer)
    operations = relationship("Operation", order_by="Operation.id", backref="transaction")

class Operation(Base):
    __tablename__ = 'transaction_operation'
    id = Column(Integer, Sequence("transaction_operation_seq"), primary_key=True)
    transaction_id = Column(String(length=32), ForeignKey('transaction.uuid'))
    operation = Column(Text())
    reverse_operation = Column(Text())
    task_runner = Column(String(length=255))
    processed = Column(Boolean, default=False)

class Storage(object):
    def __init__(self, config=None):
        """
        :type config: ConfigParser.SafeConfigParser
        """
        self.engine = create_engine(config.get('rdbms', 'database'))
        Session = sessionmaker(self.engine)
        self.session = Session()

    def sync(self):
        Base.metadata.create_all(self.engine)

    def create(self, transaction):
        # initialize a transaction inside the database
        transaction = Transaction(uuid=transaction)
        self.session.add(transaction)
        self.session.commit()

    def add_operation(self, transaction, operation, reverse_operation, task_runner):
        operation = Operation(
            transaction_id=transaction,
            operation=operation,
            reverse_operation=reverse_operation,
            task_runner=task_runner
        )
        self.session.add(operation)
        self.session.commit()

    def set_task_processed(self, transaction, operation, is_processed):
        self.session.query(Operation).filter(Operation.id == operation).update({'processed': is_processed})
        self.session.commit()

    def get_tasks(self, transaction_id):
        transaction = self.session.query(Transaction).filter(Transaction.uuid == transaction_id).one()
        return [Task(t.__dict__) for t in transaction.operations]
