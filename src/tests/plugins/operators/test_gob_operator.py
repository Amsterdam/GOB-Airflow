from unittest import TestCase
from unittest.mock import patch, MagicMock, ANY

from plugins.operators.gob_operator import GOBOperator
from plugins.operators.gob_operator import EXCHANGE, REQUEST_KEY, RESULT_KEY


class MockBaseOperator:

    def __init__(self):
        pass

class TestGOBOperator(TestCase):

    @patch('plugins.operators.gob_operator.Connection')
    @patch('plugins.operators.gob_operator.BaseOperator.__init__')
    def test_init(self, mock_init, mock_connection):
        GOBOperator()
        mock_connection.assert_called_with()
        mock_init.assert_called()

    @patch('plugins.operators.gob_operator.Connection', MagicMock())
    @patch('plugins.operators.gob_operator.BaseOperator.__init__', MagicMock())
    def test_execute(self):
        operator = GOBOperator()
        operator.job_name = "Any workflow"
        operator.step_name = "Any step"
        operator.catalogue = "Any catalogue"
        operator.collection = "Any collection"
        operator.application = "Any application"
        operator.connection = MagicMock()
        operator.connection.publish = MagicMock()

        context = MagicMock()

        context['task_instance'].xcom_pull = lambda key: None
        operator.execute(context)
        operator.connection.publish.assert_called_with(
            EXCHANGE,
            REQUEST_KEY,
            {
                "header": {
                    "catalogue": "Any catalogue",
                    "collection": "Any collection",
                    "result_key": RESULT_KEY,
                    "airflow": {
                        "dag_id": ANY,
                        "task_id": ANY,
                        "run_id": ANY
                    }
                },
                "workflow": {
                    "workflow_name": "Any workflow",
                    "step_name": "Any step",
                },
                "summary": {}
            }
        )

        context['task_instance'].xcom_pull = lambda key: {}
        operator.execute(context)
        operator.connection.publish.assert_called_with(
            EXCHANGE,
            REQUEST_KEY,
            {
                "workflow": {
                    "workflow_name": "Any workflow",
                    "step_name": "Any step",
                },
                "summary": {}
            }
        )
