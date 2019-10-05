from airflow import DAG  # Force import of DAGs

from utils.config import DEFAULT_ARGS, CATALOGUES

from utils.dag_types import relate_dag

for catalogue in CATALOGUES.values():
    dag = relate_dag(DEFAULT_ARGS,
                     catalogue=catalogue['name'])
    globals()[dag.dag_id] = dag
    for collection in catalogue['collections'].values():
        dag = relate_dag(DEFAULT_ARGS,
                         catalogue=catalogue['name'],
                         collection=collection['name'])
        globals()[dag.dag_id] = dag
