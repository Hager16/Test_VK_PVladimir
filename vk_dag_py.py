from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.time_sensor import TimeSensor
from datetime import time
import pendulum

default_args = {
    'owner': 'PVladimir',
    'start_date': pendulum.datetime(2024, 9, 21, tz='Europe/Moscow'),
    'retries': 1
}

with DAG(
        'vk_dag_py',
        default_args=default_args,
        catchup=False,
        schedule_interval='@daily'
) as dag:

    # Task 1.1: Ожидание 06:50
    wait_until_0650 = TimeSensor(
        task_id='wait_until_0650',
        target_time=time(6, 50, 0)
    )

    # Task 1: Генерация данных в 06:50
    generate_data = BashOperator(
        task_id='generate_data_task',
        bash_command='python3 /opt/airflow/scripts/generate.py /opt/airflow/input {{ execution_date.subtract(days=1).strftime("%Y-%m-%d") }} 1 10 2000; exit 0;',
        dag=dag
    )

    # Task 2.1: Ожидание 06:55
    wait_until_0655 = TimeSensor(
        task_id='wait_until_0655',
        target_time=time(6, 55, 0)
    )

    # Task 2: Агрегация данных за день в 06:55
    daily_agg = BashOperator(
        task_id='aggregate_data_task',
        bash_command='python3 /opt/airflow/scripts/day_agg.py /opt/airflow/intermediate /opt/airflow/input {{ execution_date.subtract(days=1).strftime("%Y-%m-%d") }}; exit 0;',
        dag=dag
    )

    # Task 3.1: Ожидание 07:00
    wait_until_0700 = TimeSensor(
        task_id='wait_until_0700',
        target_time=time(7, 0, 0)
    )

    # Task 3: Недельная агрегация в 07:00
    weekly_agg = BashOperator(
        task_id='weekly_aggregation_task',
        bash_command='python3 /opt/airflow/scripts/week_agg.py /opt/airflow/intermediate /opt/airflow/output {{ ds }}; exit 0;',
        dag=dag
    )

    # Определяем последовательность выполнения
    wait_until_0650 >> generate_data >> wait_until_0655 >> daily_agg >> wait_until_0700 >> weekly_agg
