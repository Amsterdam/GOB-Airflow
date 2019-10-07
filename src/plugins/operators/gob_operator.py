import logging

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from colour import Color

from utils.connection import Connection
from config.rabbitmq_config import RESULT_KEY, EXCHANGE, REQUEST_KEY

class GOBOperator(BaseOperator):
    ui_color = Color("lightgreen").hex

    @apply_defaults
    def __init__(self,
                 job_name=None,
                 step_name=None,
                 catalogue=None,
                 collection=None,
                 application=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.job_name = job_name
        self.step_name = step_name
        self.catalogue = catalogue
        self.collection = collection
        self.application = application

        self.connection = Connection()

    def execute(self, context):
        message = context['task_instance'].xcom_pull(key=context['dag_run'].run_id)

        if message is None:
            message = {
                "header": {
                    "catalogue": self.catalogue,
                    "collection": self.collection,
                    "application": self.application,
                    "result_key": RESULT_KEY,
                    "airflow": {
                        "dag_id": context['dag_run'].dag_id,
                        "task_id": context['task'].task_id,
                        "run_id": context['dag_run'].run_id
                    }
                }
            }

        message["workflow"] = {
            "workflow_name": self.job_name,
            "step_name": self.step_name,
        }

        message["summary"] = {}

        logging.info("Task started")
        self.connection.publish(EXCHANGE, REQUEST_KEY, message)
