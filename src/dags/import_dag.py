from datetime import datetime

from airflow import DAG
from airflow.operators import DummyOperator

from operators.publish import PublishMessageOperator
from sensors.consume import ConsumeMessageSensor


# Following are defaults which can be overridden later on
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 1),
    'email': ['nick.barykine@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}


dag = DAG('import_dag', default_args=default_args)

start = DummyOperator(
    task_id='start',
    dag=dag)

end = DummyOperator(
    task_id='end',
    dag=dag)


def make_message_id(action, catalogue, collection):
    return "%s_%s_%s" % (action, catalogue, collection)

def make_task_id(message_id, type):
    return "%s_%s" % (message_id, type)

def add_pipeline_to_dag(catalogue, collection, application):
    mode="reschedule"
    poke_interval=30
    timeout=60

    action_import = "import"
    message_id_import = make_message_id(action_import, catalogue, collection)

    action_compare = "compare"
    message_id_compare = make_message_id(action_compare, catalogue, collection)

    action_fullupdate = "fullupdate"
    message_id_fullupdate = make_message_id(action_fullupdate, catalogue, collection)

    import_start = PublishMessageOperator(
        task_id=make_task_id(message_id_import, "start"),
        action=action_import,
        message_id=message_id_import,
        catalogue=catalogue,
        collection=collection,
        application=application,
        dag=dag)

    import_end = ConsumeMessageSensor(
        task_id=make_task_id(message_id_import, "end"),
        action=action_import,
        message_id=message_id_import,
        mode=mode,
        poke_interval=poke_interval,
        timeout=timeout,
        dag=dag)

    compare_start = PublishMessageOperator(
        task_id=make_task_id(message_id_compare, "start"),
        action=action_compare,
        message_id=message_id_compare,
        source_action=action_import,
        source_message_id=message_id_import,
        dag=dag)

    compare_end = ConsumeMessageSensor(
        task_id=make_task_id(message_id_compare, "end"),
        action=action_compare,
        message_id=message_id_compare,
        mode=mode,
        poke_interval=poke_interval,
        timeout=timeout,
        dag=dag)

    fullupdate_start = PublishMessageOperator(
        task_id=make_task_id(message_id_fullupdate, "start"),
        action=action_fullupdate,
        message_id=message_id_fullupdate,
        source_action=action_compare,
        source_message_id=message_id_compare,
        dag=dag)

    fullupdate_end = ConsumeMessageSensor(
        task_id=make_task_id(message_id_fullupdate, "end"),
        action=action_fullupdate,
        message_id=message_id_fullupdate,
        ack=True,
        mode=mode,
        poke_interval=poke_interval,
        timeout=timeout,
        dag=dag)

    start >> import_start >> import_end \
          >> compare_start >> compare_end \
          >> fullupdate_start >> fullupdate_end \
          >> end


pipelines = [
    {
        "catalogue": "nap",
        "collection": "peilmerken",
        "application": None,
    },
    {
        "catalogue": "gebieden",
        "collection": "stadsdelen",
        "application": "DGDialog",
    },
    {
        "catalogue": "bgt",
        "collection": "onderbouw",
        "application": None,
    },
]

for pipeline in pipelines:
    add_pipeline_to_dag(
        pipeline["catalogue"],
        pipeline["collection"],
        pipeline["application"]
    )
