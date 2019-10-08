"""
GOB standard workflows: import, relate, export

"""
from operators.gob_operator import GOBOperator
from sensors.gob_sensor import GOBSensor
from utils.dag_utils import dummy_task, nyi_dag


def _start_and_wait(dag, job_name, step_name, catalogue, collection, **kwargs):
    """
    Start a workflow step and wait for the result
    """
    start = GOBOperator(
        task_id=f"{dag.dag_id}_{step_name}",
        job_name=job_name,
        step_name=step_name,
        catalogue=catalogue,
        collection=collection,
        optional=kwargs,
        dag=dag)

    wait = GOBSensor(
        task_id=f"{dag.dag_id}_{step_name}_end",
        mode='reschedule',
        poke_interval=10,
        dag=dag)

    return start, wait


def _end_of_workflow(dag, job_name):
    """
    End a workflow
    """
    return GOBOperator(
        task_id=f"{dag.dag_id}_{job_name}_end_workflow",
        dag=dag)


def _workflow(dag, job_name, step_names, catalogue, collection, **kwargs):
    """
    Populate the given dag with a workflow with steps for the given catalogue, collection and optional args
    """
    with dag:
        workflow = dummy_task(dag, "start")
        for step_name in step_names:
            start, wait = _start_and_wait(dag, job_name, step_name, catalogue, collection, **kwargs)
            workflow = workflow >> start >> wait
        workflow >> _end_of_workflow(dag, job_name) >> dummy_task(dag, "end")

    return dag


def _import_dag(dag, catalogue, collection=None, **kwargs):
    """
    Populate the given dag with an import workflow for the given catalogue, collection and optional args

    :return: the DAG instance
    """
    step_names = ["read", "update_model", "compare", "upload", "apply_events"]
    return _workflow(dag, "import", step_names, catalogue, collection, **kwargs)


def _relate_dag(dag, catalogue, collection=None, **kwargs):
    """
    Populate the given dag with a relate workflow for the given catalogue, collection and optional args

    :return: the DAG instance
    """
    step_names = ["relate", "check"]
    return _workflow(dag, "relate", step_names, catalogue, collection, **kwargs)


def _export_dag(dag, catalogue, collection=None, **kwargs):
    """
    Populate the given dag with an export workflow for the given catalogue, collection and optional args

    :return: the DAG instance
    """
    step_names = ["generate", "test"]
    return _workflow(dag, "export", step_names, catalogue, collection, **kwargs)


def get_dag_creator(dag_type):
    """
    Return a creator method for the given dag_type
    """
    return {
        'import': _import_dag,
        'relate': _relate_dag,
        'export': _export_dag
    }.get(dag_type, nyi_dag)
