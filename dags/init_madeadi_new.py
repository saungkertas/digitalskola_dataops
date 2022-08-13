from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta

with DAG('init_madeadi_new',
         schedule_interval="@once",
         start_date=datetime(2022, 7, 6)
         ) as dag:

    start = DummyOperator(
        task_id='start'
    )
    for table in ['orders_new', 'orders_details', 'products', 'customers', 'categories']:

        ingest = BashOperator(
            task_id='ingest_' + table,
            bash_command="""python3 /root/airflow/dags/ingest/madeadi/ingest_{{params.data}}.py {{ execution_date.format('YYYY-MM-DD') }}""",
            params={'data': table}

        )

        to_datalake = BashOperator(
            task_id='to_datalake_' + table,
            bash_command="""gsutil cp /root/output/madeadi/{{params.data}}/{{params.data}}_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/madeadi/staging/{{params.data}}/""",
            params={'data': table}
        )

        data_definition = BashOperator(
            task_id='data_definition_' + table,
            bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7/madeadi/staging/{{params.data}}/* > /root/table_def/madeadi/{{params.data}}.def""",
            params={'data': table}
        )

        to_dwh = BashOperator(
            task_id='to_dwh_' + table,
            bash_command="""bq mk --external_table_definition=/root/table_def/madeadi/{{params.data}}.def de_7.madeadi_{{params.data}}""",
            params={'data': table}
        )

        start >> ingest >> to_datalake >> data_definition >> to_dwh
