import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from elasticsearch import Elasticsearch
from sqlalchemy import create_engine
import pandas as pd
import psycopg2

default_args = {
    'owner': 'Emmanuel',
    'start_date': dt.datetime(2023, 9, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(hours=2)
}

def queryPostgresql():
    engine = create_engine("postgresql+psycopg2://postgres:trainings12@localhost:5432/postgres")
    # engine = engine.connect(engine)
    df=pd.read_sql("SELECT name,city FROM users",engine)
    df.to_csv('./raw_data/postgresqldata.csv')
    print("-------Data Saved------")
    
def insertElasticsearch():
    es = Elasticsearch()
    df=pd.read_csv('./raw_data/postgresqldata.csv')
    for i,r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="frompostgresql",
            doc_type="doc",body=doc)
    print(res)


with DAG(
    dag_id = 'database_insertion',
    default_args = default_args,
    schedule_interval = '@daily'
    ) as dag:
    
    get_data = PythonOperator(
        task_id = 'postgresql_data',
        python_callable = queryPostgresql
    )
    insert_data = PythonOperator(
        task_id = 'elasticsearch_data',
        python_callable = insertElasticsearch
    )
    get_data >> insert_data
