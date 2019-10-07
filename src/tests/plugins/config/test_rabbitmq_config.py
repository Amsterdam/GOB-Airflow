from unittest import TestCase

import pika

from plugins.config.rabbitmq_config import CONNECTION_PARAMS
from plugins.config.rabbitmq_config import EXCHANGE, REQUEST_KEY, RESULT_QUEUE, RESULT_KEY

class TestRabbitMQConfig(TestCase):

    def test_config(self):
        for item in [EXCHANGE, REQUEST_KEY, RESULT_QUEUE, RESULT_KEY]:
            self.assertIsInstance(item, str)

    def test_connection_params(self):
        self.assertIsInstance(CONNECTION_PARAMS, pika.ConnectionParameters)

