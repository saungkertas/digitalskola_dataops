from airflow import DAG
from airflow.operators.dummy import DummyOperatordaily_jonathan
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta

with DAG('daily_jonathan',
    schedule_interval='0 0 * * *',
    start_date=datetime(2022, 7, 1)       
) as dag:

    start = DummyOperator(
        task_id='start'
    )    
    #untuk orders
    table = ["orders" , "order_details" , "products"]
    for tables in table:
        ingest = BashOperator(
            task_id='ingest_' + tables,
            bash_command="""python3 /root/airflow/dags/ingest/jonathan/ingest_"""+tables+""".py {{ execution_date.format('YYYY-MM-DD') }}"""
        )

        to_datalake = BashOperator(
            task_id='to_datalake_' + tables,
            bash_command="""gsutil cp /root/output/jonathan/"""+tables+"""/"""+tables+"""_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/jonathan/staging/"""+tables+"""/"""
        )

    start >> ingest >> to_datalake