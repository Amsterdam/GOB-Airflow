from operators.gob_operator import GOBOperator
from sensors.gob_sensor import GOBSensor
from utils.dag_utils import dummy_task, nyi_dag

def _relate_dag(dag, catalogue, collection=None, application=None):
    dag_id = dag.dag_id
    with dag:
        start = dummy_task(dag, "start")

        relate = GOBOperator(
            task_id=f"{dag_id}_relate",
            job_name="relate",
            step_name="relate",
            catalogue=catalogue,
            collection=collection,
            application=application,
            dag=dag)

        relate_end = GOBSensor(
            task_id=f"{dag_id}_relate_end",
            mode='reschedule',
            poke_interval=10,
            dag=dag)

        check = GOBOperator(
            task_id=f"{dag_id}_check",
            job_name="relate",
            step_name="check",
            catalogue=catalogue,
            collection=collection,
            application=application,
            dag=dag)

        check_end = GOBSensor(
            task_id=f"{dag_id}_check_end",
            mode='reschedule',
            poke_interval=10,
            dag=dag)

        end_of_workflow = GOBOperator(
            task_id=f"{dag_id}_end_of_workflow",
            dag=dag)

        end = dummy_task(dag, "end")

        start >> \
        relate >> relate_end >> \
        check >> check_end >> \
        end_of_workflow >> \
        end

    return dag


def get_dag_creator(dag_type):
    return {
        'relate': _relate_dag
    }.get(dag_type, nyi_dag)
