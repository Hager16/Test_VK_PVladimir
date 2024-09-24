from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.time_sensor import TimeSensor
from datetime import time
import pendulum

# Аргументы по умолчанию для всех задач
default_args = {
    'owner': 'PVladimir',
    'start_date': pendulum.datetime(2024, 9, 21, tz='Europe/Moscow'),
    'retries': 1
}

# Создание DAG
with DAG(
        'vk_dag_spark',
        default_args=default_args,
        catchup=False,
        schedule_interval='@daily'
) as dag:

    # Task 1.1: Ожидание 06:50
    wait_until_0650 = TimeSensor(
        task_id='wait_until_0650',
        target_time=time(3, 31, 0)
    )

    # Task 1: Генерация данных в 06:50
    generate_data = BashOperator(
        task_id='generate_data_task',
        bash_command='python3 /opt/airflow/scripts/generate.py /opt/airflow/input_spark {{ execution_date.subtract(days=1).strftime("%Y-%m-%d") }} 1 10 2000',
        dag=dag
    )

    # Task 2.1: Ожидание 06:55
    wait_until_0655 = TimeSensor(
        task_id='wait_until_0655',
        target_time=time(3, 32, 0)
    )

    # Task 2: Агрегация данных за день в 06:55
    daily_agg = SparkSubmitOperator(
        task_id='aggregate_data_task',
        application='/opt/airflow/scripts/day_agg_spark.py',
        application_args=['/opt/airflow/intermediate_spark', '/opt/airflow/input_spark', '{{ execution_date.subtract(days=1).strftime("%Y-%m-%d") }}'],
        conf={"spark.master": "local[*]"},
        env_vars={
            'PYSPARK_PYTHON': '/opt/airflow/venv/bin/python',
            'SPARK_HOME': '/opt/airflow/venv/lib/python3.12/site-packages/pyspark',
            'PATH': '/opt/airflow/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
        },
        dag=dag
    )

    # Task 3.1: Ожидание 07:00
    wait_until_0700 = TimeSensor(
        task_id='wait_until_0700',
        target_time=time(3, 33, 0)
    )

    # Task 3: Недельная агрегация в 07:00
    weekly_agg = SparkSubmitOperator(
        task_id='weekly_aggregation_task',
        application='/opt/airflow/scripts/week_agg_spark.py',
        application_args=['/opt/airflow/intermediate_spark', '/opt/airflow/output_spark', '{{ ds }}'],
        conf={"spark.master": "local[*]"},
        env_vars={
            'PYSPARK_PYTHON': '/opt/airflow/venv/bin/python',
            'SPARK_HOME': '/opt/airflow/venv/lib/python3.12/site-packages/pyspark',
            'PATH': '/opt/airflow/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
        },
        dag=dag
    )


    # Определяем последовательность выполнения
    wait_until_0650 >> generate_data >> wait_until_0655 >> daily_agg >> wait_until_0700 >> weekly_agg
