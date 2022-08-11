from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

tabel_list1 = ['customers','categories','suppliers', 'products']
tabel_list2 = ['orders','order_details']

with DAG('init_sari',
    schedule_interval='@once',
    start_date=datetime(2022, 8, 1)       
) as dag:

    start = DummyOperator(
        task_id='start'
    )    

for tb1 in tabel_list1:
    ingest1= BashOperator(
        task_id='ingest_'+tb1,
        bash_command="""python3 /root/airflow/dags/ingest/sari/ingest_{{params.tb1}}.py""",
        params = {'tb1': tb1}
    )

    to_datalake1 = BashOperator(
        task_id='to_datalake_'+tb1,
        bash_command="""gsutil cp /root/output/sari/{{params.tb1}}/{{params.tb1}}.csv gs://digitalskola-de-batch7/sari/staging/{{params.tb1}}/""",
        params = {'tb1': tb1}
    )

    data_definition1 = BashOperator(
        task_id='data_definition_'+tb1,
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7/sari/staging/{{params.tb1}}/* > /root/table_def/sari/{{params.tb1}}}.def""",
        params = {'tb1': tb1}
    )

    to_dwh1 = BashOperator(
        task_id='dwh_'+tb1,
        bash_command="""bq mk --external_table_definition=/root/table_def/sari/{{params.tb1}}.def de_7.sari_{{params.tb1}}""",
        params = {'tb1': tb1}
    )
    
    start >> ingest1 >> to_datalake1 >> data_definition1 >> to_dwh1

for tb2 in tabel_list2:
    ingest2= BashOperator(
        task_id='ingest_'+tb2,
        bash_command="""python3 /root/airflow/dags/ingest/sari/ingest_{{params.tb2}}.py {{ execution_date.format('YYYY-MM-DD') }}""",
        params = {'tb2': tb2}
    )

    to_datalake2 = BashOperator(
        task_id='to_datalake_'+tb2,
        bash_command="""gsutil cp /root/output/sari/{{params.tb2}}/{{params.tb2}}_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/sari/staging/{{params.tb2}}/""",
        params = {'tb2': tb2}
    )

    data_definition2 = BashOperator(
        task_id='data_definition_'+tb2,
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7/sari/staging/{{params.tb2}}/* > /root/table_def/sari/{{params.tb2}}}.def""",
        params = {'tb2': tb2}
    )

    to_dwh2 = BashOperator(
        task_id='dwh_'+tb2,
        bash_command="""bq mk --external_table_definition=/root/table_def/sari/{{params.tb2}}.def de_7.sari_{{params.tb2}}""",
        params = {'tb2': tb2}
    )

    start >> ingest2 >> to_datalake2 >> data_definition2 >> to_dwh2
