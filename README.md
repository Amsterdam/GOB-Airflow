# GOB-Airflow

GOB Airflow orchestrates GOB data pipelines.

# Infrastructure

A running [GOB infrastructure](https://github.com/Amsterdam/GOB-Infra)
is required to run this component.

Minimal infrastructure requirements:

    - airflow database (see airflow_database docker container).
    - [RabbitMQ](http://localhost:15672/#) is up and running so Airflow can send and recieve messages to and from GOB services.

# Docker

## Requirements

* docker-compose >= 1.17
* docker ce >= 18.03

## Run

```bash
docker-compose build
```

This will:

    - Install Airflow and other requirements in docker.
    - Define Airflow home folder.
    - Copy airflow.cfg to Airflow home folder.
    - Copy all defined DAGs to Airflow dags folder.
    - Copy all plugins to Airflow plugins folder.
    - Define entry point so we can start Airflow.


```bash
docker-compose up
```

This will:

    - Initialize/upgrade Airflow database (if required).
    - Start Airflow webserver.
    - Start Airflow scheduler.


# Airflow Admin GUI

Go to [Airflow Admin GUI](http://localhost:8088/admin/) with your browser.

In Airflow Admin GUI you can start with the following:

    1. left upper corner: Set DAG: import_dag to On 
    2. Go to [Enable Import DAG](http://localhost:8088/admin/airflow/graph?dag_id=import_dag) and enable this DAG.
    3. Start DAG by clicking Trigger DAG
    4. ?? [Trigger Import DAG](http://localhost:8088/admin/airflow/trigger?dag_id=import_dag&origin=%2Fadmin%2Fairflow%2Ftree%3Fdag_id%3Dimport_dag).
