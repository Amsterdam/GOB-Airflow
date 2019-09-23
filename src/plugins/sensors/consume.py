import logging

from airflow.exceptions import AirflowException
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from operators.base import BaseMessageManager
from utils.colors import LIGHT_BLUE

log = logging.getLogger(__name__)


class ConsumeMessageSensor(BaseSensorOperator, BaseMessageManager):
    template_fields = tuple()
    ui_color = LIGHT_BLUE

    @apply_defaults
    def __init__(self, action, message_id, ack=False, *args, **kwargs):
        super(ConsumeMessageSensor, self).__init__(*args, **kwargs)
        # We use consume_queue to distinguish it from BaseOperator.queue
        self.consume_queue = "gob.workflow.jobstep.result.queue"
        self.action = action
        self.message_id = message_id
        self.key = f"{self.action}.jobstep.result"
        self.ack = ack

    def process_message(self, context, method, message, offload_id):
        if not method:
            log.info("No message yet. Waiting...")
            return False

        log.info("Message %r, offload_id %r" % (message, offload_id))

        if self.ack:
            self.ack_message(context, self.consume_queue, self.key, self.message_id)

        errors = message["summary"]["errors"]
        if errors:
            log.error("Error(s) occured. Mark task as 'failed'.")
            raise AirflowException(", ".join(errors))
        else:
            log.info("Message succeeded. Mark task as 'success'.")
            return True

    def poke(self, context):
        method, properties, message, offload_id = self.consume_message(context, self.consume_queue, self.key, self.message_id)
        return self.process_message(context, method, message, offload_id)
