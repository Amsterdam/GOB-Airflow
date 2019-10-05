import os
from datetime import datetime

import pika

MESSAGE_BROKER = os.getenv("MESSAGE_BROKER_ADDRESS", "localhost")
MESSAGE_BROKER_PORT = os.getenv("MESSAGE_BROKER_PORT", 15672)
MESSAGE_BROKER_VHOST = os.getenv("MESSAGE_BROKER_VHOST", "gob")
MESSAGE_BROKER_USER = os.getenv("MESSAGE_BROKER_USERNAME", "guest")
MESSAGE_BROKER_PASSWORD = os.getenv("MESSAGE_BROKER_PASSWORD", "guest")

CONNECTION_PARAMS = pika.ConnectionParameters(
    host=MESSAGE_BROKER,
    virtual_host=MESSAGE_BROKER_VHOST,
    credentials=pika.PlainCredentials(username=MESSAGE_BROKER_USER,
                                      password=MESSAGE_BROKER_PASSWORD),
    heartbeat_interval=1200,
    blocked_connection_timeout=600
)

EXCHANGE = "gob.workflow"
REQUEST_KEY = "workflow.request"
RESULT_QUEUE = "airflow.jobstep.result.queue"
RESULT_KEY = "airflow.result"

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 3)
}

CATALOGUES = {
    'brk': {
        'collections': {
            'meta': {}
        }
    },
    'nap': {
        'collections': {
            'peilmerken': {}
        }
    },
    'gebieden': {
        'collections': {
            'stadsdelen': {},
            'wijken': {}
        }
    }
}

for catalogue_name, catalogue in CATALOGUES.items():
    catalogue['name'] = catalogue_name
    for collection_name, collection in catalogue['collections'].items():
        collection['name'] = collection_name
