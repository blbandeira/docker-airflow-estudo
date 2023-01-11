from datetime import datetime
from airflow.models import Variable
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow import DAG

date = '{{ds}}'
load_path = '/mnt/datalake/refined/crypto_exchange_rates_daily_incremental/'

default_args = {
    'owner': 'bruno.bandeira',
    'start_date': datetime(2023, 1, 9),
}

NEW_CLUSTER = Variable.get('databricks_cluster_singleNode', deserialize_json=True)

with DAG(
    dag_id='rates_daily_incremental',
    schedule_interval='30 05 * * 1-5',
    catchup=False,
    default_args=default_args,
    tags=['DAILY', 'INCREMENTAL', 'REFINED']
) as dag:

    check_raw_data = S3KeySensor(
        task_id = 'check_raw_data_S3',
        bucket_key = fr's3://datalake-fiat20-tcc-blbb/raw/request_api_coinlayer_by_date/DT_PARTITION={date}/*.parquet',
        wildcard_match=True,
        aws_conn_id = 'aws_default_conn',
        poke_interval = 10,
        timeout = 60 * 30,
        dag=dag
    )

    run_databricks = DatabricksSubmitRunOperator(
        task_id='rates_daily_incremental',
        databricks_conn_id='databricks_default',
        dag=dag,
        new_cluster=NEW_CLUSTER,
        json={
        "run_name": "ingest_api_day",
         'notebook_task': {
            'notebook_path': '/Repos/brunolbandeira@gmail.com/datalake-tcc-fia-databricks/ETL/raw_api_request_to_context_layer',
            'base_parameters':{
            "date": date,
            "load_path": load_path} 
            }
        }
    )

    check_raw_data >> run_databricks
