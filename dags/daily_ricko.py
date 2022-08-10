from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


from datetime import datetime, timedelta

with DAG(
    'daily_ricko',
    schedule_interval='0 0 * * *',
    start_date=datetime(2022, 7, 1)
) as dag:

    start = DummyOperator(
        task_id = 'start'
    )

for table in ['orders', 'order_details', 'products']:

    ingest = BashOperator(
        task_id="ingest_" + table,
        bash_command="""python3 /root/airflow/dags/ingest/ricko/ingest_{{ params.table_name }}.py {{ execution_date.format('YYYY-MM-DD') }}""",
        params={'table_name':table}
    )

    to_datalake = BashOperator(
        task_id="to_datalake" + table,
        bash_command="""gsutil cp /root/output/ricko/{{ params.table_name }}/{{ params.table_name }}_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/ricko/staging/{{ params.table_name }}/""",
        params={'table_name':table}
    )

    start >> ingest >> to_datalake
