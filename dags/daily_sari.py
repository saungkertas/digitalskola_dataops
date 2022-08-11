from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

tabel_list = ['orders','order_details']

with DAG('daily_sari',
    schedule_interval='0 0 * * *',
    start_date=datetime(2022, 8, 1)       
) as dag:

    start = DummyOperator(
        task_id='start'
    )    


for tb in tabel_list:
    ingest= BashOperator(
        task_id='ingest_'+tb,
        bash_command="""python3 /root/airflow/dags/ingest/sari/ingest_{{params.tb}}.py {{ execution_date.format('YYYY-MM-DD') }}""",
        params = {'tb': tb}
    )

    to_datalake = BashOperator(
        task_id='to_datalake_'+tb,
        bash_command="""gsutil cp /root/output/sari/{{params.tb}}/{{params.tb}}.py_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/sari/staging/{{params.tb}}/""",
        params = {'tb': tb}
    )

    data_definition = BashOperator(
        task_id='data_definition_'+tb,
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7/sari/staging/{{params.tb}}/* > /root/table_def/sari/{{params.tb}}}.def"""
        params = {'tb': tb}
    )

    to_dwh = BashOperator(
        task_id='dwh_'+tb,
        bash_command="""bq mk --external_table_definition=/root/table_def/sari/{{params.tb}}.def de_7.sari_{{params.tb}}"""
        params = {'tb': tb}
    )
    start >> ingest >> to_datalake >> data_definition >> to_dwh
