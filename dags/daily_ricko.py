from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


from datetime import datetime, timedelta

with DAG(
    'daily_ricko',
    schedule_interval='@daily',
    start_date=datetime(2022, 7, 1)
) as dag:

    start = DummyOperator(
        task_id = 'start'
    )

    # Ingest Data   
    ingest_orders = BashOperator(
        task_id='ingest_orders',
        bash_command="""python3 /root/airflow/dags/ingest/ricko/ingest_orders.py {{ execution_date.format('YYYY-MM-DD') }}"""
    )

    ingest_order_details = BashOperator(
        task_id='ingest_order_details',
        bash_command="""python3 /root/airflow/dags/ingest/ricko/ingest_order_details.py {{ execution_date.format('YYYY-MM-DD') }}"""
    )


    # To Data Lake
    to_datalake_orders = BashOperator(
        task_id='to_datalake_orders',
        bash_command="""gsutil cp /root/output/ricko/orders/orders_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/ricko/staging/orders/"""
    )

    to_datalake_order_details = BashOperator(
        task_id='to_datalake_order_details',
        bash_command="""gsutil cp /root/output/ricko/orders_detail/orders_detail_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/ricko/staging/orders_detail/"""
    )

    start >> ingest_orders >> to_datalake_orders
    start >> ingest_order_details >> to_datalake_order_details
