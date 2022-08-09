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

    ingest_order_details = BashOperator(
        task_id='ingest_order_details',
        bash_command="""python3 /root/airflow/dags/ingest/natan/ingest_order_details.py {{ execution_date.format('YYYY-MM-DD') }}"""
    )

    to_datalake_order_details = BashOperator(
        task_id='to_datalake_order_details',
        bash_command="""gsutil cp /root/output/natan/order_details/order_details_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/natan/staging/order_details/"""
    )

    data_definition_order_details = BashOperator(
        task_id='data_definition_order_details',
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7/natan/staging/order_details/* > /root/table_def/natan/order_details.def"""
    )

    to_dwh_order_details = BashOperator(
        task_id='to_dwh_order_details',
        bash_command="""bq mk --external_table_definition=/root/table_def/natan/order_details.def de_7.natan_order_order_details"""
    )

    ingest_customers = BashOperator(
        task_id='ingest_customers',
        bash_command="""python3 /root/airflow/dags/ingest/natan/ingest_customers.py {{ execution_date.format('YYYY-MM-DD') }}"""
    )

    to_datalake_customers = BashOperator(
        task_id='to_datalake_customers',
        bash_command="""gsutil cp /root/output/natan/customers/customers_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/natan/staging/customers/"""
    )

    data_definition_customers = BashOperator(
        task_id='data_definition_customers',
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7/natan/staging/customers/* > /root/table_def/natan/customers.def"""
    )

    to_dwh_customers = BashOperator(
        task_id='to_dwh_customers',
        bash_command="""bq mk --external_table_definition=/root/table_def/natan/customers.def de_7.natan_customers"""
    )


    ingest_products = BashOperator(
        task_id='ingest_products',
        bash_command="""python3 /root/airflow/dags/ingest/natan/ingest_products.py {{ execution_date.format('YYYY-MM-DD') }}"""
    )

    to_datalake_products = BashOperator(
        task_id='to_datalake_products',
        bash_command="""gsutil cp /root/output/natan/products/products_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/natan/staging/products/"""
    )

    data_definition_products = BashOperator(
        task_id='data_definition_products',
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7/natan/staging/products/* > /root/table_def/natan/products.def"""
    )

    to_dwh_products = BashOperator(
        task_id='to_dwh_products',
        bash_command="""bq mk --external_table_definition=/root/table_def/natan/products.def de_7.natan_products"""
    )

    ingest_suppliers = BashOperator(
        task_id='ingest_suppliers',
        bash_command="""python3 /root/airflow/dags/ingest/natan/ingest_suppliers.py {{ execution_date.format('YYYY-MM-DD') }}"""
    )

    to_datalake_suppliers = BashOperator(
        task_id='to_datalake_suppliers',
        bash_command="""gsutil cp /root/output/natan/suppliers/suppliers_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/natan/staging/suppliers/"""
    )

    data_definition_suppliers = BashOperator(
        task_id='data_definition_suppliers',
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7/natan/staging/suppliers/* > /root/table_def/natan/suppliers.def"""
    )

    to_dwh_suppliers = BashOperator(
        task_id='to_dwh_suppliers',
        bash_command="""bq mk --external_table_definition=/root/table_def/natan/suppliers.def de_7.natan_suppliers"""
    )

    ingest_categories = BashOperator(
        task_id='ingest_categories',
        bash_command="""python3 /root/airflow/dags/ingest/natan/ingest_categories.py {{ execution_date.format('YYYY-MM-DD') }}"""
    )

    to_datalake_categories = BashOperator(
        task_id='to_datalake_categories',
        bash_command="""gsutil cp /root/output/natan/categories/categories_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/natan/staging/categories/"""
    )

    data_definition_categories = BashOperator(
        task_id='data_definition_categories',
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7/natan/staging/categories/* > /root/table_def/natan/categories.def"""
    )

    to_dwh_categories = BashOperator(
        task_id='to_dwh_categories',
        bash_command="""bq mk --external_table_definition=/root/table_def/natan/categories.def de_7.natan_categories"""
    )


    start >> ingest_orders >> to_datalake_orders >> data_definition_orders >> to_dwh_orders
    start >> ingest_order_details >> to_datalake_order_details >> data_definition_order_details >> to_dwh_order_details
    start >> ingest_customers >> to_datalake_customers >> data_definition_customers >> to_dwh_customers
    start >> ingest_products >> to_datalake_products >> data_definition_products >> to_dwh_products
    start >> ingest_suppliers >> to_datalake_suppliers >> data_definition_suppliers >> to_dwh_suppliers
    start >> ingest_categories >> to_datalake_categories >> data_definition_categories >> to_dwh_categories