from datetime import datetime

from airflow import DAG

from config.gob_config import CATALOGUES
from utils.dag_types import get_dag_creator
from utils.dag_utils import compose_dag, SEQUENTIAL, PARALLEL

DEFAULT_ARGS = {
    'owner': 'gob',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 3)
}


PIPELINES = ['import', 'relate', 'export']

gob_dag = DAG(dag_id="GOB",
              description=f"GOB main worfklow",
              schedule_interval=None,
              default_args=DEFAULT_ARGS)
gob_subdags = []
for pipeline in PIPELINES:
    pipeline_dag = DAG(dag_id=f"GOB.{pipeline}",
                       description=f"GOB {pipeline} worfklow",
                       schedule_interval=None,
                       default_args=DEFAULT_ARGS)
    globals()[pipeline] = pipeline_dag
    pipeline_subdags = []
    for catalogue_name, catalogue in CATALOGUES.items():
        catalogue_dag = DAG(dag_id=f"GOB.{pipeline}.{catalogue_name}",
                            schedule_interval=None,
                            default_args=DEFAULT_ARGS)
        globals()[catalogue_dag.dag_id] = catalogue_dag
        catalogue_subdags = []
        for collection_name, collection in catalogue['collections'].items():
            collection_dag = DAG(dag_id=f"GOB.{pipeline}.{catalogue_name}.{collection_name}",
                                 schedule_interval=None,
                                 default_args=DEFAULT_ARGS)
            globals()[collection_dag.dag_id] = collection_dag

            create_dag = get_dag_creator(pipeline)
            create_dag(collection_dag,
                       catalogue=catalogue_name,
                       collection=collection_name)
            catalogue_subdags.append(collection_dag)

        compose_dag(catalogue_dag, catalogue_subdags, PARALLEL, DEFAULT_ARGS)

        pipeline_subdags.append(catalogue_dag)
    compose_dag(pipeline_dag, pipeline_subdags, PARALLEL, DEFAULT_ARGS)

    gob_subdags.append(pipeline_dag)

compose_dag(gob_dag, [globals()[step] for step in PIPELINES], SEQUENTIAL, DEFAULT_ARGS)
