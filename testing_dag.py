from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'apache_airflow',
    'depeends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '@daily',
    'retries': 1,
    'retry_delay': timedelta(seconds=20)
    
}

dag = DAG(
    dag_id = 'ETL_DAG',
    default_args = default_args,
    description = 'etl using bash operator',
    schedule_interval = timedelta(days=1)
)
#Download the data stored on the provided link
download=BashOperator(
    task_id='download',
    bash_command='curl "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"  > /opt/airflow/dags/web_server_log.txt',
    dag=dag,
)
#Extract two columns from the file downloaded
extract=BashOperator(
    task_id='extract',
    bash_command='cut -f1,4 -d"#" \
    /opt/airflow/dags/web_server_log.txt > \
    /opt/airflow/dags/web_server_log_extracted.txt',
    dag=dag,
)

#Transform the extracted columns to be all lowercase
transform=BashOperator(
    task_id='transform',
    bash_command='tr  "[A-Z]" "[a-z]"  < \
    /opt/airflow/dags/web_server_log_extracted.txt > \
    /opt/airflow/dags/Transformed.txt',
    dag=dag,
)
#Compress the transformed and extracted data
load=BashOperator(
    task_id='load',
    bash_command='tar cfv /opt/airflow/dags/web_server_log_processing.tar  \
    /opt/airflow/dags/web_server_log_extracted.txt \
    /opt/airflow/dags/Transformed.txt',
    dag=dag,
)
download >> extract >> transform >> load