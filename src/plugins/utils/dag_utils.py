"""
DAG utils

Various DAG utility methods
"""
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

# Execution type of SubDAGs within a DAG
SEQUENTIAL = "sequential"
PARALLEL = "parallel"


def compose_dag(dag, subdags, mode, default_args):
    """
    Populate the given dag with the given subdags

    Subdags can be executed sequentially or in parallel
    """
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
    return dag


def dummy_task(dag, name):
    """
    Returns a dummy task for the given dag. The name is used to construct the task_id of the task
    """
    return DummyOperator(task_id=f"{dag.dag_id}_{name}", dag=dag)


def nyi_dag(dag, *args, **kwargs):
    """
    Populates the given dag with a dummy start and end task
    """
    with dag:
        start = dummy_task(dag, "start")
        end = dummy_task(dag, "end")
        start >> end

    return dag
