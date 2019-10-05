import pika
import json

from utils.config import RESULT_QUEUE, RESULT_KEY, EXCHANGE, CONNECTION_PARAMS

class Connection:
    DELIVERY_TAG = '_delivery_tag'

    def __init__(self):
        self.connection = pika.BlockingConnection(CONNECTION_PARAMS)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=RESULT_QUEUE, durable=True)
        self.channel.queue_bind(
            exchange=EXCHANGE,
            queue=RESULT_QUEUE,
            routing_key=RESULT_KEY
        )

    def publish(self, exchange, key, message):
        body = json.dumps(message)
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=key,
            properties=pika.BasicProperties(
                delivery_mode=2  # Make messages persistent
            ),
            body=body
        )

    def consume(self, queue):
        for method, properties, body in self.channel.consume(queue=queue, inactivity_timeout=10):
            if method is None:
                break
            message = json.loads(body)
            message[self.DELIVERY_TAG] = method.delivery_tag
            yield message
        self.channel.cancel()
        yield None

    def ack(self, message):
        delivery_tag = message[self.DELIVERY_TAG]
        self.channel.basic_ack(delivery_tag=delivery_tag)
        del message[self.DELIVERY_TAG]
