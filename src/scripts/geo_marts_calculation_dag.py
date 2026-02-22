import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
}

dag_spark = DAG(
    dag_id="geo_marts_calculation",
    default_args=default_args,
    schedule_interval=None,
    params={
        'date': '2022-06-21',  # значение по умолчанию
        'sample': '0.0005'         # значение по умолчанию
    }
)

# Витрина пользователей
users_mart = SparkSubmitOperator(
    task_id='users_mart',
    dag=dag_spark,
    application='/home/kirillprsv/run_marts_job.py',
    conn_id='yarn_spark',
    application_args=[
        "{{ params.date }}",
        "{{ params.sample }}"
    ],
    conf={"spark.driver.maxResultSize": "20g"},
    executor_cores=1,
    executor_memory='1g',
    deploy_mode='client'
)

# Витрина зон
zones_mart = SparkSubmitOperator(
    task_id='zones_mart',
    dag=dag_spark,
    application='/home/kirillprsv/run_marts_job.py',
    conn_id='yarn_spark',
    application_args=[
        "{{ params.date }}",
        "{{ params.sample }}"
    ],
    conf={"spark.driver.maxResultSize": "20g"},
    executor_cores=1,
    executor_memory='1g',
    deploy_mode='client'
)

# Витрина друзей
friends_mart = SparkSubmitOperator(
    task_id='friends_mart',
    dag=dag_spark,
    application='/home/kirillprsv/run_marts_job.py',
    conn_id='yarn_spark',
    application_args=[
        "{{ params.date }}",
        "{{ params.sample }}"
    ],
    conf={"spark.driver.maxResultSize": "20g"},
    executor_cores=1,
    executor_memory='1g',
    deploy_mode='client'
)

users_mart >> zones_mart >> friends_mart