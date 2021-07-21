import logging
from cassandra.policies import RetryPolicy
from cassandra import WriteType as WT

WriteType = WT

logging.getLogger('cassandra').setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)


class CassandraStoreConnectionRetryPolicy(RetryPolicy):

     def on_write_timeout(self, query, consistency, write_type,
                          required_responses, received_responses, retry_num):
        """
        By default, failed write operations will retried at most once, and
        they will only be retried if the `write_type` was
        :attr:`~.WriteType.BATCH_LOG or SIMPLE`.
        """
        logger.debug("Write timeout policy applied num retry %i, write_type %i", retry_num, write_type)
        if retry_num != 0:
            return self.RETHROW, None
        elif write_type == WriteType.BATCH_LOG or write_type == WriteType.SIMPLE:
            return self.RETRY, consistency
        else:
            return self.RETHROW, None





