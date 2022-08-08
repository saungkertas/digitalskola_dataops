from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta

with DAG('init_natan',
    schedule_interval="@once",
    start_date=datetime(2022, 7, 6)       
) as dag:

    start = DummyOperator(
        task_id='start'
    )    
      
    ingest_orders = BashOperator(
        task_id='ingest_orders',
        bash_command="""python3 /root/airflow/dags/ingest/natan/ingest_orders.py {{ execution_date.format('YYYY-MM-DD') }}"""
    )

    to_datalake_orders = BashOperator(
        task_id='to_datalake_orders',
        bash_command="""gsutil cp /root/output/natan/orders/orders_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/natan/staging/orders/"""
    )

    data_definition_orders = BashOperator(
        task_id='data_definition_orders',
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7/natan/staging/orders/* > /root/table_def/natan/orders.def"""
    )

    to_dwh_orders = BashOperator(
        task_id='to_dwh_orders',
        bash_command="""bq mk --external_table_definition=/root/table_def/natan/orders.def de_7.natan_orders"""
    )

    
    start >> ingest_orders >> to_datalake_orders >> data_definition_orders >> to_dwh_orders
    start >> ingest_order_details >> to_datalake_order_details >> data_definition_order_details >> to_dwh_order_details
    start >> ingest_customers >> to_datalake_customers >> data_definition_customers >> to_dwh_customers
    start >> ingest_products >> to_datalake_products >> data_definition_products >> to_dwh_products
    start >> ingest_suppliers >> to_datalake_suppliers >> data_definition_suppliers >> to_dwh_suppliers
    start >> ingest_categories >> to_datalake_categories >> data_definition_categories >> to_dwh_categories