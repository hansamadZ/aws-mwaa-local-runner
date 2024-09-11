import pytest
import datetime as datetime_
from airflow import DAG
from datetime import datetime

pytest_plugins = ["helpers_namespace"]

@pytest.fixture
def test_dag():
    return DAG(
        "test_dag",
        default_args={"owner": "airflow"},
        start_date=datetime(2024, 9, 11),
        schedule_interval=datetime_.timedelta(days=1)
    )

@pytest.helpers.register
def run_task(task, dag):
    dag.clear()
    task.run(
        start_date=dag.start_date,
        end_date=dag.end_date
    )
