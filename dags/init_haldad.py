from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta

with DAG('init_haldad',
    schedule_interval="@once",
    start_date=datetime(2022, 7, 6)       
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    for task in ['orders', 'order_details', 'products']:    
        
        ingest = BashOperator(
            task_id='ingest_' + task,
            taskName={'name':task},
            bash_command="""python3 /root/airflow/dags/ingest/haldad/ingest_{{ taskName.name }}.py {{ execution_date.format('YYYY-MM-DD') }}""",
        )

        to_datalake = BashOperator(
            task_id='to_datalake_' + task,
            taskName={'name':task},
            bash_command="""gsutil cp /root/output/haldad/{{ taskName.name }}/{{ taskName.name }}_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/haldad/staging/{{ taskName.name }}/""",
        )

        data_definition = BashOperator(
            task_id='data_definition_' + task,
            taskName={'name':task},
            bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7/haldad/staging/{{ taskName.name }}/* > /root/table_def/haldad/{{ taskName.name }}.def""",
        )

        to_dwh = BashOperator(
            task_id='to_dwh_orders',
            taskName={'name':task},
            bash_command="""bq mk --external_table_definition=/root/table_def/haldad/{{ taskName.name }}.def de_7.haldad_{{ taskName.name }}""",
        )

        start >> ingest >> to_datalake >> data_definition >> to_dwh