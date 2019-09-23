import logging

from gobcore.message_broker.initialise_queues import initialize_message_broker
from gobcore.message_broker.config import CONNECTION_PARAMS, WORKFLOW_EXCHANGE
from gobcore.message_broker.message_broker import Connection

log = logging.getLogger(__name__)


class BaseMessageManager:
    def consume_message(self, context, queue, key, message_id, ack=False):

        def message_matched(msg):
            header = msg["header"]
            return header["dag_id"] == context['dag_run'].dag_id \
                and header["run_id"] == context['dag_run'].run_id \
                and header["message_id"] == message_id

        params = {"load_message": False}
        with Connection(CONNECTION_PARAMS, params) as connection:
            return connection.consume(
                exchange=WORKFLOW_EXCHANGE,
                queue=queue,
                key=key,
                message_matched=message_matched,
                ack=ack
            )

    def ack_message(self, context, queue, key, message_id):
        return self.consume_message(context, queue, key, message_id, ack=True)

    def publish_message(self, queue, message):
        with Connection(CONNECTION_PARAMS) as connection:
            log.info("Publishing message: exchange %r, queue %r, message %r" % (WORKFLOW_EXCHANGE, queue, message))
            connection.publish(WORKFLOW_EXCHANGE, queue, message)
