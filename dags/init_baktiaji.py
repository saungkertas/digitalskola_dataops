from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta

with DAG('init_baktiaji',
    schedule_interval="@once",
    start_date=datetime(2022, 7, 6)
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    # Ingest Data
    ingest_orders = BashOperator(
        task_id='ingest_orders',
        bash_command="""python3 /root/airflow/dags/ingest/baktiaji/ingest_orders.py {{ execution_date.format('YYYY-MM-DD') }}"""
    )

    ingest_products = BashOperator(
        task_id='ingest_products',
        bash_command="""python3 /root/airflow/dags/ingest/baktiaji/ingest_products.py {{ execution_date.format('YYYY-MM-DD') }}"""
    )

    ingest_suppliers = BashOperator(
        task_id='ingest_suppliers',
        bash_command="""python3 /root/airflow/dags/ingest/baktiaji/ingest_suppliers.py {{ execution_date.format('YYYY-MM-DD') }}"""
    )

    ingest_categories = BashOperator(
        task_id='ingest_categories',
        bash_command="""python3 /root/airflow/dags/ingest/baktiaji/ingest_categories.py {{ execution_date.format('YYYY-MM-DD') }}"""
    )

    ingest_customers = BashOperator(
        task_id='ingest_customers',
        bash_command="""python3 /root/airflow/dags/ingest/baktiaji/ingest_customers.py {{ execution_date.format('YYYY-MM-DD') }}"""
    )



    # To Data Lake
    to_datalake_orders = BashOperator(
        task_id='to_datalake_orders',
        bash_command="""gsutil cp /root/output/baktiaji/orders/orders_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/baktiaji/staging/orders/"""
    )

    to_datalake_products = BashOperator(
        task_id='to_datalake_products',
        bash_command="""gsutil cp /root/output/baktiaji/products/products_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/baktiaji/staging/products/"""
    )

    to_datalake_suppliers = BashOperator(
        task_id='to_datalake_suppliers',
        bash_command="""gsutil cp /root/output/baktiaji/suppliers/suppliers_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/baktiaji/staging/suppliers/"""
    )

    to_datalake_categories = BashOperator(
        task_id='to_datalake_categories',
        bash_command="""gsutil cp /root/output/baktiaji/categories/categories_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/baktiaji/staging/categories/"""
    )

    to_datalake_customers = BashOperator(
        task_id='to_datalake_customers',
        bash_command="""gsutil cp /root/output/baktiaji/customers/customers_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/baktiaji/staging/customers/"""
    )



    #Data Definition
    data_definition_orders = BashOperator(
        task_id='data_definition_orders',
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7/baktiaji/staging/orders/* > /root/table_def/baktiaji/orders.def"""
    )

    data_definition_products = BashOperator(
        task_id='data_definition_products',
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7/baktiaji/staging/products/* > /root/table_def/baktiaji/products.def"""
    )

    data_definition_suppliers = BashOperator(
        task_id='data_definition_suppliers',
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7/baktiaji/staging/suppliers/* > /root/table_def/baktiaji/suppliers.def"""
    )

    data_definition_categories = BashOperator(
        task_id='data_definition_categories',
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7/baktiaji/staging/categories/* > /root/table_def/baktiaji/categories.def"""
    )

    data_definition_customers = BashOperator(
        task_id='data_definition_customers',
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7/baktiaji/staging/customers/* > /root/table_def/baktiaji/customers.def"""
    )



    #To Dwh
    to_dwh_orders = BashOperator(
        task_id='to_dwh_orders',
        bash_command="""bq mk --external_table_definition=/root/table_def/baktiaji/orders.def de_7.baktiaji_orders"""
    )

    to_dwh_products = BashOperator(
        task_id='to_dwh_products',
        bash_command="""bq mk --external_table_definition=/root/table_def/baktiaji/products.def de_7.baktiaji_products"""
    )

    to_dwh_suppliers = BashOperator(
        task_id='to_dwh_suppliers',
        bash_command="""bq mk --external_table_definition=/root/table_def/baktiaji/suppliers.def de_7.baktiaji_suppliers"""
    )

    to_dwh_categories = BashOperator(
        task_id='to_dwh_categories',
        bash_command="""bq mk --external_table_definition=/root/table_def/baktiaji/categories.def de_7.baktiaji_categories"""
    )

    to_dwh_customers = BashOperator(
        task_id='to_dwh_customers',
        bash_command="""bq mk --external_table_definition=/root/table_def/baktiaji/customers.def de_7.baktiaji_customers"""
    )

    start >> ingest_orders >> to_datalake_orders >> data_definition_orders >> to_dwh_orders
    start >> ingest_products >> to_datalake_products >> data_definition_products >> to_dwh_products
    start >> ingest_suppliers >> to_datalake_suppliers >> data_definition_suppliers >> to_dwh_suppliers
    start >> ingest_categories >> to_datalake_categories >> data_definition_categories >> to_dwh_categories
    start >> ingest_customers >> to_datalake_customers >> data_definition_customers >> to_dwh_customers
