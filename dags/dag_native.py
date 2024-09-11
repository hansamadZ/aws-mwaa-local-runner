import json
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
#from operators.Extract.ExtractOperator import ExtractOperator

def extract_data(data_string: str):
    return json.loads(data_string)

with DAG(
    "native_dag",
    default_args = {
        'owner': 'airflow',
    }
) as dag:
    """
        Add documentation here
    """
    extractor = PythonOperator(task_id="extractor",
                               python_callable=extract_data,
                               op_kwargs={"data_string": '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'})




