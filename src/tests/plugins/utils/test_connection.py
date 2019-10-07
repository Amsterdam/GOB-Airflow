from unittest import TestCase
from unittest.mock import patch, MagicMock

from plugins.utils.connection import Connection


class MockMethod():
    def __init__(self, delivery_tag):
        self.delivery_tag = delivery_tag


class TestConnection(TestCase):

    @patch('plugins.utils.connection.pika', MagicMock())
    def test_init(self):
        connection = Connection()
        self.assertIsNotNone(connection.connection)
        self.assertIsNotNone(connection.channel)

    @patch('plugins.utils.connection.pika', MagicMock())
    def test_publish(self):
        connection = Connection()
        connection.channel.basic_publish = MagicMock()
        connection.publish("any exchange", "any key", {'a': 1})
        connection.channel.basic_publish.assert_called()

    @patch('plugins.utils.connection.pika', MagicMock())
    def test_consume_end(self):
        connection = Connection()
        connection.channel.consume = lambda queue, inactivity_timeout: iter([(None, None, None)])
        connection.channel.cancel = MagicMock()
        n = 0
        for msg in connection.consume('any queue'):
            self.assertIsNone(msg)
            n += 1
        connection.channel.cancel.assert_called()

    @patch('plugins.utils.connection.pika', MagicMock())
    def test_consume(self):
        connection = Connection()
        connection.channel.consume = lambda queue, inactivity_timeout: iter([
            (MockMethod('Any delivery tag'), 'Any properties', '{}'),
            (None, None, None)
        ])
        connection.channel.cancel = MagicMock()
        n = 0
        for msg in connection.consume('any queue'):
            if msg is not None:
                self.assertEqual(msg['_delivery_tag'], 'Any delivery tag')
                n += 1
        self.assertEqual(n, 1)
        connection.channel.cancel.assert_called()

    @patch('plugins.utils.connection.pika', MagicMock())
    def test_ack(self):
        connection = Connection()
        connection.channel.basic_ack = MagicMock()
        msg = {
            '_delivery_tag': 'Any delivery tag'
        }
        result = connection.ack(msg)
        self.assertIsNone(msg.get('_delivery_tag'))
        connection.channel.basic_ack.assert_called_with(delivery_tag='Any delivery tag')

