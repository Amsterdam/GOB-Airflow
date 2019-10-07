"""
GOB Sensor

The GOB sensor listens to GOB workflow messages
"""
import logging

from airflow.exceptions import AirflowException
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from colour import Color

from utils.connection import Connection
from config.rabbitmq_config import RESULT_QUEUE


class GOBSensor(BaseSensorOperator):
    ui_color = Color("lime").hex

    @apply_defaults
    def __init__(self, *args, **kwargs):
        """
        Initializes the GOB sensor with the GOB-RabbitMQ connection
        """
        super().__init__(*args, **kwargs)

        self.connection = Connection()

    def poke(self, context):
        """
        Try to read workflow messages for the current task (DAG run_id)
        :param context: Airflow context
        :return: final result message for the current task or None if not yet available
        """
        # todo force reschedule when no result received
        result = None
        for msg in self.connection.consume(RESULT_QUEUE):
            if msg is None:
                logging.info("No message yet. Waiting...")
                return None

            if msg['header']['airflow']["run_id"] != context['dag_run'].run_id:
                logging.info("Skip message for other workflow")
                continue

            self.connection.ack(msg)

            status = msg.get('status')
            if status is not None:
                logging.info(f"Status: {status}")
                continue

            summary = msg.get('summary')
            if summary is not None:
                result = self.handle_result(context, msg)
                continue

        return result

    def handle_result(self, context, msg):
        """
        Handle a result message for the current task
        """
        logging.info("Result received")
        errors = msg["summary"]["errors"]
        warnings = msg["summary"]["warnings"]
        if warnings:
            logging.warning(f"Task warnings ({len(warnings)}):")
            logging.warning("\n".join(warnings))
        if errors:
            logging.warning(f"Task errors ({len(errors)}):")
            logging.error("\n".join(errors))
            raise AirflowException("Task has failed")
        # Save result message for any successor task
        context['task_instance'].xcom_push(key=context['dag_run'].run_id, value=msg)
        return msg
