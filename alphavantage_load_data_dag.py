import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from util import flat_json_data
# from airflow.operators.sensors import HttpSensor
from datetime import datetime, timedelta
import json
# from airflow.providers.http.operators.http import HttpOperator
# from airflow.providers.http.sensors.http import HttpSensor
import os
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.trigger_rule import TriggerRule

# ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
alphavantage_con = os.environ.get("alphavantage_con")
gcp_con = os.environ.get('gcp-connection')

def save_ibm_file(ti) -> None: 
    data_ibm = ti.xcom_pull(task_ids=['get_op_ibm'])
    # print("Symbol--------", data_ibm[0].get("Meta Data", {}).get("2. Symbol", ""))
    import pandas as pd

    df = pd.DataFrame(flat_json_data(data_ibm[0]))
    df.to_csv("ibm_data.csv", index=None)
    # with open('ibm_data.json', 'w') as fp:
    #     json.dump(flat_json_data(data_ibm[0]), fp)

def save_reliance_file(ti) -> None:
    data_reliance = ti.xcom_pull(task_ids=['get_op_reliance'])
    
    import pandas as pd
    df = pd.DataFrame(flat_json_data(data_reliance[0]))
    df.to_csv("reliance_data.csv", index=None)

    # with open('reliance_data.json', 'w') as fp:
    #     json.dump(flat_json_data(data_reliance[0]), fp)


def upload_to_gcs(data_folder,gcs_path,**kwargs):
    data_folder = os.getcwd()
    bucket_name = 'poc-bucket-data-transfer'  # Your GCS bucket name
    gcs_conn_id = 'gcp-connection'

    # List all CSV files in the data folder
    # Note : you can filter the files extentions with file.endswith('.csv')
    # Examples : file.endswith('.csv')
    #            file.endswith('.json')
    #            file.endswith('.csv','json')

    csv_files = [file for file in os.listdir(data_folder) if file.endswith('.csv')]
    print("CSV FIles -------", csv_files)
    
    # Upload each CSV file to GCS
    for csv_file in csv_files:
        local_file_path = os.path.join(data_folder, csv_file)
        gcs_file_path = f"{gcs_path}/{csv_file}"
        upload_task = LocalFilesystemToGCSOperator(
            task_id=f'upload_to_gcs',
            src=local_file_path,
            dst=gcs_file_path,
            bucket=bucket_name,
            gcp_conn_id=gcs_conn_id,
        )
        upload_task.execute(context=kwargs) 
        os.remove(local_file_path)


with DAG(
    dag_id="alphavantage_http_api",
    default_args={ "retries": 0, "owner": "airflow"},
    tags=["example","http"],
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False
    ) as dag1:

    run_this_first = DummyOperator(
            task_id='run_this_first',
        )

    task_get_op_ibm = SimpleHttpOperator(
        task_id="get_op_ibm",
        method="GET",
        http_conn_id='alphavantage_con',
        endpoint="query?function=TIME_SERIES_DAILY&symbol=IBM&outputsize=full&apikey=demo",
        headers={"Content-Type": "application/json"},
        # xcom_push=True,
        # dag=dag,
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    ) 

    save_ibm_data = PythonOperator(
        task_id='save_ibm_data',
        python_callable=save_ibm_file
    )

    task_get_op_reliance = SimpleHttpOperator(
        task_id="get_op_reliance",
        method="GET",
        http_conn_id='alphavantage_con',
        endpoint="https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=RELIANCE.BSE&outputsize=full&apikey=demo",
        headers={"Content-Type": "application/json"},
        # xcom_push=True
        # dag=dag,
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    ) 

    save_reliance_data = PythonOperator(
        task_id='save_reliance_data',
        python_callable=save_reliance_file
    )

    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        op_args=['', 'gcs/destination'],
        provide_context=True,
        # dag=dag,  # Assign the DAG to the task
    )

    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery",
        # conn_id='gcp-connection',
        # google_cloud_storage_conn_id='gcp-connection',
        # gcp_conn_id='gcp-connection',
        bucket="poc-bucket-data-transfer",
        source_objects=["gcs/destination/*.csv"],
        # source_format="JSON",
        destination_project_dataset_table="airflow_test.stock_market_data",
        schema_fields=[
            {"name": "open", "type": "STRING", "mode": "NULLABLE"},
            {"name": "high", "type": "STRING", "mode": "NULLABLE"},
            {"name": "low", "type": "STRING", "mode": "NULLABLE"},
            {"name": "close", "type": "STRING", "mode": "NULLABLE"},
            {"name": "volume", "type": "STRING", "mode": "NULLABLE"},
            {"name": "symbol", "type": "STRING", "mode": "NULLABLE"},
            {"name": "date", "type": "STRING", "mode": "NULLABLE"},
            {"name": "adjusted_close", "type": "STRING", "mode": "NULLABLE"},
            {"name": "dividend_amount", "type": "STRING", "mode": "NULLABLE"},
            {"name": "split_coefficient", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
        external_table=False,
        autodetect=True,
        # max_id_key=MAX_ID_STR,
        # deferrable=True,
    )

    # run_this_first >> [task_get_op_ibm, task_get_op_reliance]
    run_this_first >> task_get_op_ibm >> save_ibm_data
    run_this_first >> task_get_op_reliance >> save_reliance_data
    [save_ibm_data, save_reliance_data] >> upload_to_gcs_task >> gcs_to_bigquery


# from tests.system.utils import get_test_run  # noqa: E402

# # Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
# test_run = get_test_run(dag)