import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta

from jobs.kafka_stream import kafka_stream


default_args = {
    'owner': 'Mek',
    'retries': 1,
    'retry_delay': timedelta(seconds=3)
}

with DAG(

    default_args = default_args,
    dag_id = 'user_automation',
    description = 'Stream data into kafka topic "users_created"',
    start_date = airflow.utils.dates.days_ago(1),
    schedule_interval = None,
    catchup = False

)as dag:
    
    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable = kafka_stream,
        op_kwargs = {
            'api_name':'randomuser',
            'bootstrap_servers':'broker:29092',
            'topic':'users_created'
        }
    )

    # Task dependencies
    streaming_task
