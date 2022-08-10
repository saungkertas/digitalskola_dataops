from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta

with DAG('init_ricko',
    schedule_interval="@once",
    start_date=datetime(2022, 7, 6)       
) as dag:

    start = DummyOperator(
        task_id='start'
    )

for table in ['orders', 'order_details', 'products']:    
      
    ingest = BashOperator(
        task_id='ingest_' + table,
        bash_command="""python3 /root/airflow/dags/ingest/ricko/ingest_{{ params.table_name }}.py {{ execution_date.format('YYYY-MM-DD') }}""",
        params={'table_name':table}
    )

    to_datalake = BashOperator(
        task_id='to_datalake_' + table,
        bash_command="""gsutil cp /root/output/ricko/{{ params.table_name }}/{{ params.table_name }}_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/ricko/staging/{{ params.table_name }}/""",
        params={'table_name':table}
    )

    data_definition = BashOperator(
        task_id='data_definition_' + table,
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7/ricko/staging/{{ params.table_name }}/* > /root/table_def/ricko/{{ params.table_name }}.def""",
        params={'table_name':table}
    )

    to_dwh = BashOperator(
        task_id='to_dwh_' + table,
        bash_command="""bq mk --external_table_definition=/root/table_def/ricko/{{ params.table_name }}.def de_7.ricko_{{ params.table_name }}""",
        params={'table_name':table}
    )

    start >> ingest >> to_datalake >> data_definition >> to_dwh