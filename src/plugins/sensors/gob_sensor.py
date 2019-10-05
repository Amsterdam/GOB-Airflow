from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from colour import Color

from utils.connection import Connection
from utils.config import RESULT_QUEUE


class GOBSensor(BaseSensorOperator):
    ui_color = Color("lime").hex

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.connection = Connection()

    def poke(self, context):
        # todo force reschedule when no result received
        result = None
        for msg in self.connection.consume(RESULT_QUEUE):
            if msg is None:
                print("No message yet. Waiting...")
                return True

            if msg['header']['airflow']["run_id"] != context['dag_run'].run_id:
                print("Skip message for other workflow")
                continue

            self.connection.ack(msg)

            status = msg.get('status')
            if status is not None:
                print("Status", status)
                continue

            # todo fail on errors
            summary = msg.get('summary')
            if summary is not None:
                print("Result received")
                result = msg
                context['task_instance'].xcom_push(key=context['dag_run'].run_id, value=msg)
                continue

        return result
