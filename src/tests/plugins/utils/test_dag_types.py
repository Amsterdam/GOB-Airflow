from unittest import TestCase
from unittest.mock import patch, MagicMock

from plugins.utils.dag_types import _import_dag, _relate_dag, _export_dag, get_dag_creator


class TestDagTypes(TestCase):

    @patch('plugins.utils.dag_types.GOBOperator')
    @patch('plugins.utils.dag_types.GOBSensor')
    @patch('plugins.utils.dag_types.dummy_task', MagicMock())
    @patch('plugins.utils.dag_types.nyi_dag', MagicMock())
    def test_import_dag(self, mock_sensor, mock_operator):
        dag = MagicMock()
        dag.dag_id = "Any dagid"
        result = _import_dag(dag, "Any catalogue", "Any collection")
        self.assertEqual(dag, result)

        self.assertEqual(len(mock_operator.call_args_list), 6)
        self.assertEqual(len(mock_sensor.call_args_list), 5)

        jobs = [call[1].get('job_name', '') + call[1].get('step_name', '') for call in mock_operator.call_args_list]
        self.assertEqual(jobs, ['importread', 'importupdate_model', 'importcompare', 'importupload', 'importapply_events', ''])

    @patch('plugins.utils.dag_types.GOBOperator')
    @patch('plugins.utils.dag_types.GOBSensor')
    @patch('plugins.utils.dag_types.dummy_task', MagicMock())
    @patch('plugins.utils.dag_types.nyi_dag', MagicMock())
    def test_relate_dag(self, mock_sensor, mock_operator):
        dag = MagicMock()
        dag.dag_id = "Any dagid"
        result = _relate_dag(dag, "Any catalogue", "Any collection")
        self.assertEqual(dag, result)

        self.assertEqual(len(mock_operator.call_args_list), 3)
        self.assertEqual(len(mock_sensor.call_args_list), 2)

        jobs = [call[1].get('job_name', '') + call[1].get('step_name', '') for call in mock_operator.call_args_list]
        self.assertEqual(jobs, ['relaterelate', 'relatecheck', ''])

    @patch('plugins.utils.dag_types.GOBOperator')
    @patch('plugins.utils.dag_types.GOBSensor')
    @patch('plugins.utils.dag_types.dummy_task', MagicMock())
    @patch('plugins.utils.dag_types.nyi_dag', MagicMock())
    def test_export_dag(self, mock_sensor, mock_operator):
        dag = MagicMock()
        dag.dag_id = "Any dagid"
        result = _export_dag(dag, "Any catalogue", "Any collection")
        self.assertEqual(dag, result)

        self.assertEqual(len(mock_operator.call_args_list), 3)
        self.assertEqual(len(mock_sensor.call_args_list), 2)

        jobs = [call[1].get('job_name', '') + call[1].get('step_name', '') for call in mock_operator.call_args_list]
        self.assertEqual(jobs, ['exportgenerate', 'exporttest', ''])

    @patch('plugins.utils.dag_types.nyi_dag')
    def test_get_dag_creator(self, mock_nyi):
        self.assertEqual(get_dag_creator('any dag'), mock_nyi)
        self.assertEqual(get_dag_creator('relate'), _relate_dag)
