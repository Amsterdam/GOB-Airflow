from unittest import TestCase
from unittest.mock import patch, MagicMock, ANY

from plugins.utils.dag_utils import compose_dag, dummy_task, nyi_dag
from plugins.utils.dag_utils import SEQUENTIAL, PARALLEL


class MockDAG():
    def __init__(self, dag_id):
        self.dag_id = dag_id


class Test_DAG_Utils(TestCase):

    @patch('plugins.utils.dag_utils.dummy_task', MagicMock())
    @patch('plugins.utils.dag_utils.SubDagOperator')
    def test_compose_dag(self, mock_subdag):
        dag = MagicMock()
        subdag = MagicMock()
        result = compose_dag(dag, [subdag], SEQUENTIAL, "Any default args")
        mock_subdag.assert_called_with(dag=dag,
                                       default_args="Any default args",
                                       schedule_interval=None,
                                       subdag=subdag,
                                       task_id=ANY
                                       )
        self.assertEqual(result, dag)
        result = compose_dag(dag, [subdag], PARALLEL, "Any default args")
        mock_subdag.assert_called_with(dag=dag,
                                       default_args="Any default args",
                                       schedule_interval=None,
                                       subdag=subdag,
                                       task_id=ANY
                                       )
        self.assertEqual(result, dag)

    @patch('plugins.utils.dag_utils.DummyOperator', lambda task_id, dag: task_id)
    def test_dummy_task(self):
        result = dummy_task(MockDAG('Any id'), 'Any name')
        self.assertEqual(result, "Any id_Any name")

    @patch('plugins.utils.dag_utils.dummy_task')
    def test_nyi_dag(self, mock_dummy_task):
        dag = MagicMock()
        result = nyi_dag(dag)
        self.assertEqual(result, dag)
        dummies = [name for dag, name in [call[0] for call in mock_dummy_task.call_args_list]]
        self.assertEqual(dummies, ['start', 'end'])
