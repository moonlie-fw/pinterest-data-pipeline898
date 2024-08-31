from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 

notebook_task = {
    'notebook_path': 'Users/moonliefong@gmail.com/pinterest-data-pipeline898',  
}


notebook_params = {
    "Variable": 5  
}

default_args = {
    'owner': '0afff86fcd1b', 
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3, 
    'retry_delay': timedelta(minutes=2)
}

with DAG('0afff86fcd1b_dag',
    start_date=datetime(2023, 8, 1), 
    schedule_interval='@daily', 
    catchup=False,
    default_args=default_args
    ) as dag:

    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    opr_submit_run