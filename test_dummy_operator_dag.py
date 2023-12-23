from airflow import DAG 
import airflow
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retry_delay": timedelta(minutes=45),
    "start_date": datetime(2021,1,1),
    "retries": 0
}

dag = DAG(dag_id="test_pramod_dag", schedule_interval='@once', default_args=default_args, catchup=False)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)

start >> end