from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from operators.gob_operator import GOBOperator
from sensors.gob_sensor import GOBSensor

def relate_dag(default_args, catalogue, collection=None, application=None):
    id = "_".join([name for name in [catalogue, collection, application] if name])
    dag_id = f"Relate_{id}"
    dag = DAG(dag_id,
              schedule_interval=None,
              default_args=default_args)

    with dag:
        relate_start = DummyOperator(
            task_id=f'relate_start_{id}',
            dag=dag)

        relate_relate = GOBOperator(
            task_id=f"relate_relate_{id}",
            job_name="relate",
            step_name="relate",
            catalogue=catalogue,
            collection=collection,
            application=application,
            dag=dag)

        relate_relate_end = GOBSensor(
            task_id=f"relate_relate_end_{id}",
            mode='reschedule',
            poke_interval=10,
            dag=dag)

        relate_check = GOBOperator(
            task_id=f"relate_check_{id}",
            job_name="relate",
            step_name="check",
            catalogue=catalogue,
            collection=collection,
            application=application,
            dag=dag)

        relate_check_end = GOBSensor(
            task_id=f"relate_check_end_{id}",
            mode='reschedule',
            poke_interval=10,
            dag=dag)

        relate_end = GOBOperator(
            task_id=f"relate_end_{id}",
            dag=dag)

        relate_start >> \
        relate_relate >> relate_relate_end >> \
        relate_check >> relate_check_end >> \
        relate_end

    return dag
