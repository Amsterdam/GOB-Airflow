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

dag = DAG('import_dag', default_args=default_args)

start = DummyOperator(
    task_id='start',
    dag=dag)

import_nap_peilmerken_start = PublishMessageOperator(
    task_id="import_nap_peilmerken_start",
    action="import",
    message_id="import_nap_peilmerken",
    catalogue="nap",
    collection="peilmerken",
    dag=dag)

import_nap_peilmerken_end = ConsumeMessageSensor(
    task_id='import_nap_peilmerken_end',
    action="import",
    message_id="import_nap_peilmerken",
    mode='reschedule',
    poke_interval=30,
    timeout=60,
    dag=dag)

compare_nap_peilmerken_start = PublishMessageOperator(
    task_id="compare_nap_peilmerken_start",
    action="compare",
    message_id="compare_nap_peilmerken",
    source_action="import",
    source_message_id="import_nap_peilmerken",
    dag=dag)

compare_nap_peilmerken_end = ConsumeMessageSensor(
    task_id='compare_nap_peilmerken_end',
    action="compare",
    message_id="compare_nap_peilmerken",
    mode='reschedule',
    poke_interval=30,
    timeout=60,
    dag=dag)

fullupdate_nap_peilmerken_start = PublishMessageOperator(
    task_id="fullupdate_nap_peilmerken_start",
    action="fullupdate",
    message_id="fullupdate_nap_peilmerken",
    source_action="compare",
    source_message_id="compare_nap_peilmerken",
    dag=dag)

fullupdate_nap_peilmerken_end = ConsumeMessageSensor(
    task_id='fullupdate_nap_peilmerken_end',
    action="fullupdate",
    message_id="fullupdate_nap_peilmerken",
    ack=True,
    mode='reschedule',
    poke_interval=30,
    timeout=60,
    dag=dag)

# # gebieden:stadsdelen
import_gebieden_stadsdelen_start = PublishMessageOperator(
    task_id="import_gebieden_stadsdelen_start",
    action="import",
    message_id="import_gebieden_stadsdelen",
    catalogue="gebieden",
    collection="stadsdelen",
    application="DGDialog",
    dag=dag)

import_gebieden_stadsdelen_end = ConsumeMessageSensor(
    task_id='import_gebieden_stadsdelen_end',
    action="import",
    message_id="import_gebieden_stadsdelen",
    mode='reschedule',
    poke_interval=30,
    timeout=60,
    dag=dag)

compare_gebieden_stadsdelen_start = PublishMessageOperator(
    task_id="compare_gebieden_stadsdelen_start",
    action="compare",
    message_id="compare_gebieden_stadsdelen",
    source_action="import",
    source_message_id="import_gebieden_stadsdelen",
    dag=dag)

compare_gebieden_stadsdelen_end = ConsumeMessageSensor(
    task_id='compare_gebieden_stadsdelen_end',
    action="compare",
    message_id="compare_gebieden_stadsdelen",
    mode='reschedule',
    poke_interval=30,
    timeout=60,
    dag=dag)

fullupdate_gebieden_stadsdelen_start = PublishMessageOperator(
    task_id="fullupdate_gebieden_stadsdelen_start",
    action="fullupdate",
    message_id="fullupdate_gebieden_stadsdelen",
    source_action="compare",
    source_message_id="compare_gebieden_stadsdelen",
    dag=dag)

fullupdate_gebieden_stadsdelen_end = ConsumeMessageSensor(
    task_id='fullupdate_gebieden_stadsdelen_end',
    action="fullupdate",
    message_id="fullupdate_gebieden_stadsdelen",
    ack=True,
    mode='reschedule',
    poke_interval=30,
    timeout=60,
    dag=dag)


end = DummyOperator(
    task_id='end',
    dag=dag)


start >> import_nap_peilmerken_start >> import_nap_peilmerken_end \
      >> compare_nap_peilmerken_start >> compare_nap_peilmerken_end \
      >> fullupdate_nap_peilmerken_start >> fullupdate_nap_peilmerken_end \
      >> end

start >> import_gebieden_stadsdelen_start >> import_gebieden_stadsdelen_end \
      >> compare_gebieden_stadsdelen_start >> compare_gebieden_stadsdelen_end \
      >> fullupdate_gebieden_stadsdelen_start >> fullupdate_gebieden_stadsdelen_end \
      >> end
