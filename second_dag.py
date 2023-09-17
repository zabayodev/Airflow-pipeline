from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'emma',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id = 'first_dag_v4',
    default_args = default_args,
    description = 'Fist dag',
    start_date = datetime(2023, 7, 1, 2),
    schedule_interval = '@daily'
) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command = 'date'
    )
    task2 = BashOperator(
        task_id = 'second_task',
        bash_command = "echo hey, this is the second task"
    )
    task3 = BashOperator(
        task_id = 'third_task',
        bash_command = "echo hey, i am task3 following task2"
    )
    task1 >> [task2, task3]
    # task1 >> task3
