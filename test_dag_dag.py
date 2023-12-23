from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def first_function_execution(**context):
    name = context.get("Name", "No key found")
    print(f"Hello {name} !!")
    context['ti'].xcom_push(key="mykey", value="hello from first_function_execution")


def second_function_execution(**context):
    instance = context.get('ti').xcom_pull(key="mykey")
    print(f'Inside second function,, {instance}')



with DAG(
    dag_id="second_dag",
    schedule_interval="@daily",
    default_args={
        "owner":"airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=45),
        "start_date": datetime(2022, 5, 1)},
    catchup=False) as f:
        first_function_execution = PythonOperator(
            task_id="first_function_execution",
            python_callable=first_function_execution,
            provide_context=True,
            op_kwargs={"Name": "Pramod Gupta"}
        )

        second_function_execution = PythonOperator(
            task_id="second_function_execution",
            python_callable=second_function_execution,
            provide_context=True,
        )


first_function_execution >> second_function_execution