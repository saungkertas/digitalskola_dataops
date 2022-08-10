from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta

with DAG('daily_haldad',
    schedule_interval='0 0 * * *',
    start_date=datetime(2022, 8, 1)       
) as dag:

    start = DummyOperator(
        task_id='start'
    )    
    

    for task in ['orders','order_details','products']:

        ingest = BashOperator(
            task_id=f'ingest_{task}',
            taskName = {'name':task},
            bash_command="""python3 /root/airflow/dags/ingest/haldad/ingest_{{taksName.name}}.py {{ execution_date.format('YYYY-MM-DD') }}"""
        )

        to_datalake = BashOperator(
            task_id='to_datalake_orders',
            task = {'name':task},
            bash_command="""gsutil cp /root/output/haldad/{{taskName.name}}/{{taskName.name}}_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/haldad/staging/{{taskName.name}}/"""
        )
        start >> ingest >> to_datalake