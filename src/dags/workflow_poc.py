from datetime import datetime

from airflow import DAG
from airflow.operators import DummyOperator

from operators.publish import PublishMessageOperator
from sensors.consume import ConsumeMessageSensor


# Following are defaults which can be overridden later on
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 18),
    'email': ['nick.barykine@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG('workflow_poc', default_args=default_args)

start = DummyOperator(
    task_id='start',
    dag=dag)

import_test_delete_start = PublishMessageOperator(
    task_id="import_test_delete_start",
    action="import",
    message_id="import_test_delete",
    catalogue="test_catalogue",
    collection="test_entity",
    application="DELETE_ALL",
    dag=dag)

import_test_delete_end = ConsumeMessageSensor(
    task_id='import_test_delete_end',
    action="import",
    message_id="import_test_delete",
    mode='reschedule',
    poke_interval=30,
    dag=dag)

import_test_add_start = PublishMessageOperator(
    task_id="import_test_add_start",
    action="import",
    message_id="import_test_add",
    source_action="import",
    source_message_id="import_test_delete",
    dag=dag)

import_test_add_end = ConsumeMessageSensor(
    task_id='import_test_add_end',
    action="import",
    message_id="import_test_add",
    mode='reschedule',
    poke_interval=30,
    dag=dag)

import_test_modify1_start = PublishMessageOperator(
    task_id="import_test_modify1_start",
    action="import",
    message_id="import_test_modify1",
    source_action="import",
    source_message_id="import_test_add",
    dag=dag)

import_test_modify1_end = ConsumeMessageSensor(
    task_id='import_test_modify1_end',
    action="import",
    message_id="import_test_modify1",
    mode='reschedule',
    poke_interval=30,
    dag=dag)

export_test_start = PublishMessageOperator(
    task_id="export_test_start",
    action="export",
    message_id="export_test",
    source_action="import",
    source_message_id="import_test_modify1",
    dag=dag)

export_test_end = ConsumeMessageSensor(
    task_id='export_test_end',
    action="export",
    message_id="export_test",
    ack=True,
    mode='reschedule',
    poke_interval=30,
    timeout=60*2,
    dag=dag)

end = DummyOperator(
    task_id='end',
    dag=dag)

start >> import_test_delete_start >> import_test_delete_end \
      >> import_test_add_start >> import_test_add_end \
      >> import_test_modify1_start >> import_test_modify1_end \
      >> export_test_start >> export_test_end \
      >> end
