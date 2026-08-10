from pathlib import Path
from unittest import TestCase

from airflow.models import DagBag

DAG_FILE = Path(__file__).parents[1] / "dags" / "parallel_steps_dag.py"
DATA_SOURCES = {"customers", "orders", "payments", "products"}
QUALITY_CHECKS = {"completeness", "freshness", "consistency"}


class ParallelStepsDagTest(TestCase):
    def test_dag_loads_with_production_shaped_graph(self):
        dagbag = DagBag(dag_folder=str(DAG_FILE), include_examples=False)
        dag = dagbag.get_dag("parallel_steps")
        extract_tasks = {f"extract.extract_{source}" for source in DATA_SOURCES}
        transform_tasks = {f"transform.transform_{source}" for source in DATA_SOURCES}
        quality_tasks = {f"quality.check_{check}" for check in QUALITY_CHECKS}

        self.assertEqual(dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(
            set(dag.task_ids),
            {
                "start",
                "validate_inputs",
                *extract_tasks,
                *transform_tasks,
                "aggregate_metrics",
                *quality_tasks,
                "publish_dataset",
                "notify_success",
                "finish",
            },
        )
        self.assertEqual(
            dag.get_task("validate_inputs").downstream_task_ids,
            extract_tasks,
        )
        self.assertEqual(
            dag.get_task("aggregate_metrics").upstream_task_ids,
            transform_tasks,
        )
        self.assertEqual(
            dag.get_task("publish_dataset").upstream_task_ids,
            quality_tasks,
        )
