"""
GOB Dags

GOB Dags are created for each catalogue and collection
and for each of the workflows import, relate and export
automatically.

The DAGs are bundled in subdags and finally integrated in a GOB DAG that unites them all.
"""
from datetime import datetime

from airflow import DAG

from config.gob_config import CATALOGUES, PIPELINES, DEFAULT_PIPELINE_ARGS
from utils.dag_types import get_dag_creator
from utils.dag_utils import compose_dag, SEQUENTIAL, PARALLEL

# Default DAG create arguments
DEFAULT_ARGS = {
    'owner': 'gob',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 3)
}

# Main GOB DAG
gob_dag = DAG(dag_id="GOB",
              description=f"GOB main workflow",
              schedule_interval=None,
              default_args=DEFAULT_ARGS)
gob_subdags = []
for pipeline in PIPELINES:
    # Pipeline DAG, eg import, relate or export
    pipeline_dag = DAG(dag_id=f"GOB.{pipeline}",
                       description=f"GOB {pipeline} workflow",
                       schedule_interval=None,
                       default_args=DEFAULT_ARGS)
    globals()[pipeline] = pipeline_dag
    pipeline_subdags = []

    for catalogue_name, catalogue in CATALOGUES.items():
        # Catalogue DAG, eg Meetbouten, Gebieden
        catalogue_dag = DAG(dag_id=f"GOB.{pipeline}.{catalogue_name}",
                            schedule_interval=None,
                            default_args=DEFAULT_ARGS)
        globals()[catalogue_dag.dag_id] = catalogue_dag
        catalogue_subdags = []

        for collection_name, collection in catalogue['collections'].items():
            # Collection DAG, eg peilmerken, wijken
            collection_dag = DAG(dag_id=f"GOB.{pipeline}.{catalogue_name}.{collection_name}",
                                 schedule_interval=None,
                                 default_args=DEFAULT_ARGS)
            globals()[collection_dag.dag_id] = collection_dag

            # Get the DAG for the pipeline, eg the relate pipeline
            create_dag = get_dag_creator(pipeline)
            create_dag(collection_dag,
                       catalogue=catalogue_name,
                       collection=collection_name,
                       **DEFAULT_PIPELINE_ARGS[pipeline])
            catalogue_subdags.append(collection_dag)

        # Execute collections within catalogue in parallel
        compose_dag(catalogue_dag, catalogue_subdags, PARALLEL, DEFAULT_ARGS)
        pipeline_subdags.append(catalogue_dag)

    # Execute catalogues within a pipeline in parallel
    compose_dag(pipeline_dag, pipeline_subdags, PARALLEL, DEFAULT_ARGS)
    gob_subdags.append(pipeline_dag)

# Handle pipeline steps sequential
compose_dag(gob_dag, [globals()[step] for step in PIPELINES], SEQUENTIAL, DEFAULT_ARGS)
