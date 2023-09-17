import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd

def csv_to_json():
    df=pd.read_csv('./raw_data/data.csv')
    for i, r in df.iterrows():
        print(r['name'])
    df.to_json('./raw_data/fromairflowjson.json', orient='records')

default_args = {
    'owner': 'Emmanuel',
    'start_date': dt.datetime(2023, 9, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5)
}

with DAG(
    dag_id = 'csv_convertion_for_day',
    default_args = default_args,
    schedule_interval = '@daily'
    ) as dag:
    
    printing_task = BashOperator(
        task_id = 'starting',
        bash_command = 'echo "I am reading the csv now ..."',
    )
    
    csvtojson = PythonOperator(
        task_id = 'converting_csv_to_json',
        python_callable = csv_to_json
    )
    printing_task >> csvtojson