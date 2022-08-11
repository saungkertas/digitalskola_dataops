from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta

with DAG('daily_salma',
    schedule_interval='0 0 * * *',
    start_date=datetime(2022, 7, 1)       
) as dag:

    start = DummyOperator(
        task_id='start'
    )    
      
    ingest_orders = BashOperator(
        task_id='ingest_orders',
        bash_command="""python3 /root/airflow/dags/ingest/salma/ingest_orders.py {{ execution_date.format('YYYY-MM-DD') }}"""
    )

    ingest_order_details = BashOperator(
        task_id='ingest_order_details',
        bash_command="""python3 /root/airflow/dags/ingest/salma/ingest_order_details.py {{ execution_date.format('YYYY-MM-DD') }}"""
    )

    to_datalake_orders = BashOperator(
        task_id='to_datalake_orders',
        bash_command="""gsutil cp /root/output/salma/orders/orders_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/salma/staging/orders/"""
    )

    to_datalake_order_details = BashOperator(
        task_id='to_datalake_orders',
        bash_command="""gsutil cp /root/output/salma/order_details/order_details{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/salma/staging/orders/"""
    )
    start >> ingest_orders >> to_datalake_orders
    start >> ingest_order_details >> to_datalake_order_details