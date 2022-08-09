from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta

with DAG('init_jonathan',
    schedule_interval="@once",
    start_date=datetime(2022, 7, 6)       
) as dag:

    start = DummyOperator(
        task_id='start'
    )    
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

        data_definition = BashOperator(
            task_id='data_definition_' + tables,
            bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7/jonathan/staging/"""+tables+"""/* > /root/table_def/jonathan/"""+tables+""".def"""
        )

        to_dwh = BashOperator(
            task_id='to_dwh_' + tables,
            bash_command="""bq mk --external_table_definition=/root/table_def/jonathan/"""+tables+""".def de_7.jonathan_"""+tables
        )

    start >> ingest >> to_datalake >> data_definition >> to_dwh