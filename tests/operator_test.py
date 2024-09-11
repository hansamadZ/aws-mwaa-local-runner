import pytest
import json
from datetime import datetime
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
#from operators.Extract.ExtractOperator import ExtractOperator

def write_context_to_temp(context: str, tmpfile: str, tmpdir):
    with open(tmpdir / tmpfile, "w") as f:
        f.write(context)

def read_context_from_temp(tmpfile: str, tmpdir):
    with open(tmpdir / tmpfile, "r") as f:
        return f.readlines()[0]

def extract_data(data_string: str, tmpfile:str, tmpdir, **kwargs):
    context = kwargs
    write_context_to_temp(context=context["dag_run"].run_id, tmpfile=tmpfile, tmpdir=tmpdir)
    return json.loads(data_string)

def transform_data(tmpfile:str, tmpdir, **kwargs):
    return

#@pytest.mark.skip
def test_extract_data(test_dag, tmpdir):
    tmpfile = "run_id.txt"
    extractor = PythonOperator(task_id="extractor",
                               python_callable=extract_data,
                               op_kwargs={ "data_string": '{"1001": 301.27, "1002": 433.21, "1003": 502.22}',
                                           "tmpfile": tmpfile,
                                           "tmpdir": tmpdir
                                           },
                               dag=test_dag)
    pytest.helpers.run_task(task=extractor, dag=test_dag)
    ti = TaskInstance(task=extractor, run_id=read_context_from_temp(tmpfile, tmpdir))
    result = ti.xcom_pull(task_ids="extractor")

    assert result == { "1001": 301.27, "1002": 433.21, "1003": 502.22 }

