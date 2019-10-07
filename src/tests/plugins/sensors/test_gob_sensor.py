from unittest import TestCase
from unittest.mock import patch, MagicMock, ANY

from plugins.sensors.gob_sensor import GOBSensor
from plugins.sensors.gob_sensor import RESULT_QUEUE
from plugins.sensors.gob_sensor import AirflowException


class MockBaseOperator:

    def __init__(self):
        pass

class TestGOBSensor(TestCase):

    @patch('plugins.sensors.gob_sensor.Connection')
    @patch('plugins.sensors.gob_sensor.BaseSensorOperator.__init__')
    def test_init(self, mock_init, mock_connection):
        GOBSensor()
        mock_connection.assert_called_with()
        mock_init.assert_called()

    @patch('plugins.sensors.gob_sensor.Connection', MagicMock())
    @patch('plugins.sensors.gob_sensor.BaseSensorOperator.__init__', MagicMock())
    @patch('plugins.sensors.gob_sensor.logging')
    def test_poke(self, mock_logging):
        sensor = GOBSensor()
        context = MagicMock()

        sensor.connection.consume = lambda q: iter([None])
        result = sensor.poke(context)
        self.assertIsNone(result)

        context['dag_run'].run_id = "Any other run id"
        msg = {
            'header': {
                'airflow': {
                    'run_id': "My own run id"
                }
            }
        }
        sensor.connection.consume = lambda q: iter([msg])
        result = sensor.poke(context)
        self.assertIsNone(result)
        mock_logging.info.assert_called_with("Skip message for other workflow")

        context['dag_run'].run_id = "My own run id"
        result = sensor.poke(context)
        sensor.connection.ack.assert_called()

        msg['status'] = "Any status"
        result = sensor.poke(context)
        mock_logging.info.assert_called_with('Status: Any status')

        del msg['status']
        msg['summary'] = {
            'warnings': ['Any warning'],
            'errors': []
        }
        result = sensor.poke(context)
        mock_logging.info.assert_called_with('Result received')
        mock_logging.warning.assert_called_with('Any warning')

        msg['summary']['errors'] = ['Any error']
        with self.assertRaises(AirflowException):
            result = sensor.poke(context)
        mock_logging.error.assert_called_with('Any error')