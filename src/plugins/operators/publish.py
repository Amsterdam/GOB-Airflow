import logging

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from operators.base import BaseMessageManager
from utils.colors import LIGHT_PINK

log = logging.getLogger(__name__)


class PublishMessageOperator(BaseOperator, BaseMessageManager):
    ui_color = LIGHT_PINK

    @apply_defaults
    def __init__( \
            self,
            action,
            message_id,
            source_action=None,
            source_message_id=None,
            catalogue=None,
            collection=None,
            application=None,
            entity=None,
            source=None,
            destination=None,
            *args,
            **kwargs):
        super().__init__(*args, **kwargs)
        self.action = action
        # We use publish_queue and consume_queue to distinguish them from BaseOperator.queue
        self.publish_queue = f"{self.action}.request"
        self.message_id = message_id

        self.source_action=source_action
        self.consume_queue = "gob.workflow.jobstep.result.queue"
        if self.source_action:
            self.key = f"{self.source_action}.jobstep.result"
        else:
            self.key = None
        self.source_message_id=source_message_id

        self.catalogue = catalogue
        self.collection = collection
        self.application = application
        self.entity = entity
        self.source = source
        self.destination = destination

    def init_message(self, context):
        return {
            "header": {
                "dag_id": context['dag_run'].dag_id,
                "task_id": context['task'].task_id,
                "run_id": context['dag_run'].run_id,
                "message_id": self.message_id,
                "catalogue": self.catalogue,
                "collection": self.collection,
                "application": self.application,
                "entity": self.entity,
                "source": self.source,
                "destination": self.destination,
                "version": '0.1',
                "job_id": context['task_instance'].job_id,
                "process_id": context['task_instance'].pid,
                "timestamp": context['ts']
            },
            "summary": {
            },
            "contents": []
        }

    def update_message(self, context, message):
        # we do separate update of the header to update (merge) this nested dict properly
        log.info("Source message %r" % message)
        header = message["header"]
        new_header = {
                "dag_id": context['dag_run'].dag_id,
                "task_id": context['task'].task_id,
                "run_id": context['dag_run'].run_id,
                "message_id": self.message_id,
                "job_id": context['task_instance'].job_id,
                "process_id": context['task_instance'].pid,
                "timestamp": context['ts']
            }
        header.update(new_header)
        message.update({"header": header})
        message.update({"summary": {}})
        log.info("New message %r" % message)
        return message

    def update_source_message(self, context, queue, key, message_id):
        method, properties, message, offload_id = self.consume_message(context, queue, key, message_id)
        if method:
            message = self.update_message(context, message)
            self.ack_message(context, queue, key, message_id)
            return message
        else:
            error_message = "No source message found for queue %s, key %s, message_id %s" % (queue, key, message_id)
            log.error(error_message)
            raise AirflowException(error_message)

    def create_message(self, context, action, queue, key, message_id):
        if action and message_id:
            message = self.update_source_message(context, queue, key, message_id)
        else:
            message = self.init_message(context)
        return message

    def execute(self, context):
        message = self.create_message(
            context,
            self.source_action,
            self.consume_queue,
            self.key,
            self.source_message_id
        )
        self.publish_message(self.publish_queue, message)
