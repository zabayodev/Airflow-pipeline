from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import os


#getting dag directory path
dag_path = os.getcwd()

def transform_data():
    booking = pd.read_csv("./raw_data/booking.csv", low_memory=False)
    client = pd.read_csv("./raw_data/client.csv", low_memory=False)
    hotel = pd.read_csv("./raw_data/hotel.csv", low_memory=False)

    #merging booking and client data
    data = pd.merge(booking, client, on='client_id')
    data.rename(columns={'name':'client_name', 'type':'client_type'}, inplace=True)
    
    #merging booking, client and hotel togenther
    data = pd.merge(data, hotel, on='hotel_id')
    data.rename(columns={'name': 'hotel_name'}, inplace=True)
    
    #formatting date
    data.booking_date = pd.to_datetime(data.booking_date, dayfirst=False, format='mixed', infer_datetime_format=True)
    
    #converting all the currency
    data.loc[data.currency == 'EUR', ['booking_cost']] = data.booking_cost * 0.8
    data.currency.replace("EUR", "GBP", inplace=True)
    
    #remove unneccessary columns
    data = data.drop('address', axis=1)
    
    #load the precessed data
    data.to_csv("./raw_data/processed_data.csv", index=False)
    
def load_data():
    engine = create_engine(f'postgresql://localhost/postgres')
    c = engine.connect()
    c.execute('''
               CREATE TABLE IF NOT EXISTS booking_record (
                    client_id INTEGER NOT NULL,
                    booking_date TEXT NOT NULL,
                    room_type TEXT(512) NOT NULL,
                    hotel_id INTEGER NOT NULL,
                    booking_cost NUMERIC,
                    currency TEXT,
                    age INTEGER,
                    client_name TEXT(512),
                    client_type TEXT(512),
                    hotel_name TEXT(512)
                );
              ''')
    records = pd.read_csv("./raw_data/preocessed_data.csv")
    records.to_sql('booking_records', conn, if_exists='replace', index=False)


#dag initialization in apache
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5)
}

with DAG(
    dag_id = 'Booking_ingestion',
    default_args = default_args,
    description = 'building booking dag',
    schedule_interval = timedelta(days=1),
    catchup = False
) as dag:
    task_1 = PythonOperator(
        task_id = 'transform_data',
        python_callable = transform_data
)

    task_2 = PythonOperator(
        task_id = 'load_data',
        python_callable = load_data
)
task_1 >> task_2