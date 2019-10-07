from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

SEQUENTIAL = "sequential"
PARALLEL = "parallel"

def compose_dag(dag, subdags, mode, default_args):
    dag_id = dag.dag_id
    with dag:
        start = dummy_task(dag, "start")

        subtasks = []
        for subdag in subdags:
            subdag_id = subdag.dag_id
            task_id = subdag_id[len(dag_id) + 1:]
            subtask = SubDagOperator(
                task_id=task_id,
                schedule_interval=None,
                subdag=subdag,
                default_args=default_args,
                dag=dag
            )
            subtasks.append(subtask)

        if mode == SEQUENTIAL:
            lasttask = start
            for task in subtasks:
                lasttask >> task
                lasttask = task
        elif mode == PARALLEL:
            lasttask = start >> subtasks

        end = dummy_task(dag, "end")

        lasttask >> end


def dummy_task(dag, name):
    return DummyOperator(task_id=f"{dag.dag_id}_{name}", dag=dag)


def nyi_dag(dag, catalogue, collection=None, application=None):
    dag_id = dag.dag_id
    with dag:
        start = dummy_task(dag, "start")

        end = dummy_task(dag, "end")

        start >> end

    return dag
