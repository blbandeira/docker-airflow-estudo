from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow import DAG

ACCESS_KEY = Variable.get('api-access-key')
date = '{{ds}}'
load_path = '/mnt/datalake/raw/request_api_coinlayer_by_date/'

default_args = {
    'owner': 'bruno.bandeira',
    'start_date': datetime(2023, 1, 4),
}

NEW_CLUSTER = Variable.get('databricks_cluster_singleNode', deserialize_json=True)

with DAG(
    dag_id='ingestion_api_by_date',
    schedule_interval='00 05 * * 1-5',
    catchup=False,
    default_args=default_args,
    tags=['DAILY', 'INCREMENTAL', 'RAW']
) as dag:

    run_databricks = DatabricksSubmitRunOperator(
        task_id='ingestion_api_by_date',
        databricks_conn_id='databricks_default',
        dag=dag,
        new_cluster=NEW_CLUSTER,
        json={
        "run_name": "ingestion_api_by_date",
         'notebook_task': {
            'notebook_path': '/Repos/brunolbandeira@gmail.com/datalake-tcc-fia-databricks/Ingestion/ingest-api',
            'base_parameters':{
            "ACCESS_KEY": ACCESS_KEY,
            "date": date,
            "load_path": load_path} 
            }
        }
    )

    run_databricks
