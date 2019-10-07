"""
Connection logic

This module contains functionality to connect to RabbitMQ exchanges and queues
and publish, consume and acknowledge messages
"""
import pika
import json

from config.rabbitmq_config import RESULT_QUEUE, RESULT_KEY, EXCHANGE, CONNECTION_PARAMS


class Connection:
    DELIVERY_TAG = '_delivery_tag'
    CONSUME_INACTIVITY_TIMEOUT = 10

    def __init__(self):
        """
        Initializes a connection with the message broker

        The required queue and binding is created if they don't yet exist
        """
        self.connection = pika.BlockingConnection(CONNECTION_PARAMS)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=RESULT_QUEUE, durable=True)
        self.channel.queue_bind(
            exchange=EXCHANGE,
            queue=RESULT_QUEUE,
            routing_key=RESULT_KEY
        )

    def publish(self, exchange, key, message):
        """
        Publish a message to the given exchange using the given routing key
        :return: publish result
        """
        body = json.dumps(message)
        return self.channel.basic_publish(
            exchange=exchange,
            routing_key=key,
            properties=pika.BasicProperties(
                delivery_mode=2  # Make messages persistent
            ),
            body=body
        )

    def consume(self, queue):
        """
        Generator that yields messages from the given queue.
        Each message is populated with its delivery tag to allow for later acknowledgement

        :return: message or None is no new messages can be read
        """
        for method, properties, body in self.channel.consume(queue=queue,
                                                             inactivity_timeout=self.CONSUME_INACTIVITY_TIMEOUT):
            if method is None:
                break
            message = json.loads(body)
            message[self.DELIVERY_TAG] = method.delivery_tag
            yield message
        self.channel.cancel()
        yield None

    def ack(self, message):
        """
        Acknowledge a message (removingit from the queue) that has previously been consumed

        :param message: a message previously received by the consume method
        :return:
        """
        delivery_tag = message[self.DELIVERY_TAG]
        self.channel.basic_ack(delivery_tag=delivery_tag)
        del message[self.DELIVERY_TAG]
        return message
