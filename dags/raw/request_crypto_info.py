from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow import DAG

ACCESS_KEY = Variable.get('api-access-key')
date = '{{ds}}'
load_path = '/mnt/datalake/raw/crypto_info_snapshot/'

default_args = {
    'owner': 'bruno.bandeira',
    'start_date': datetime(2023, 1, 10),
}

NEW_CLUSTER = Variable.get('databricks_cluster_singleNode', deserialize_json=True)

with DAG(
    dag_id='crypto_info_dimension',
    schedule_interval='00 05 * * 1',
    catchup=False,
    default_args=default_args,
    tags=['WEEKLY', 'SNAPSHOT', 'RAW']
) as dag:

    run_databricks = DatabricksSubmitRunOperator(
        task_id='ingestion_api_crypto_info',
        databricks_conn_id='databricks_default',
        dag=dag,
        new_cluster=NEW_CLUSTER,
        json={
        "run_name": "ingestion_api_crypto_info",
         'notebook_task': {
            'notebook_path': '/Repos/brunolbandeira@gmail.com/datalake-tcc-fia-databricks/Ingestion/crypto_dimension',
            'base_parameters':{
            "ACCESS_KEY": ACCESS_KEY,
            "date": date,
            "load_path": load_path} 
            }
        }
    )

    run_databricks
