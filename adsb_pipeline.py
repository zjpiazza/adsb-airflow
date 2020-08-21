from datetime import timedelta
import datetime
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook

import boto3
from psycopg2.extras import RealDictCursor
import json

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
dag = DAG(
    'adsb-etl',
    default_args=default_args,
    description='ETL pipeline for extracting beacon data from Postgres and uploading to S3 for persistence',
    schedule_interval=timedelta(days=1),
)


def get_beacons_for_previous_day(**kwargs):
    """
    Get all of the beacons generated the previous day

    :param ds:
    :param kwargs:
    :return:
    """

    pg_hook = PostgresHook(postgres_conn_id='adsb1090', schema='adsb1090')
    previous_day = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    select_cmd = f"SELECT id, hex, timestamp, flight, altitude, speed, heading, lat, lon FROM beacons WHERE  date(to_timestamp(timestamp)) = date('{previous_day}');"
    connection = pg_hook.get_conn()
    cursor = connection.cursor(cursor_factory=RealDictCursor)
    cursor.execute(select_cmd)
    return (previous_day, cursor.fetchall())


def upload_file_to_s3(**kwargs):
    """
    Create an in-memory file and upload the data to S3 for persistence

    :param ds:
    :param kwargs:
    :return:
    """
    ti = kwargs['ti']
    previous_day, beacons = ti.xcom_pull(key=None, task_ids='get_beacons_for_previous_day')
    s3hook = S3Hook(aws_conn_id='adsb_aws')
    client = s3hook.get_conn()
    client.put_object(
        Body=str(json.dumps(beacons, indent=2)),
        Bucket='piaware-adsb-data',
        Key=f'ads-b-dump-{previous_day}.json'
    )


def delete_prior_data(**kwargs):
    """
    Delete the beacons that were just uploaded

    :param ds:
    :param kwargs:
    :return:
    """
    ti = kwargs['ti']
    previous_day, beacons = ti.xcom_pull(key=None, task_ids='get_beacons_for_previous_day')
    pg_hook = PostgresHook(postgres_conn_id='adsb1090', schema='adsb1090')
    delete_cmd = f"DELETE FROM beacons WHERE date(to_timestamp(timestamp)) = date('{previous_day}');"
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(delete_cmd)
    conn.commit()
    conn.close()


task1 = PythonOperator(
    task_id='get_beacons_for_previous_day',
    provide_context=False,
    python_callable=get_beacons_for_previous_day,
    dag=dag)

task2 = PythonOperator(
    task_id='upload_file_to_s3',
    provide_context=True,
    python_callable=upload_file_to_s3,
    dag=dag)

task3 = PythonOperator(
    task_id='delete_prior_data',
    provide_context=True,
    python_callable=delete_prior_data,
    dag=dag)

task1 >> task2 >> task3
