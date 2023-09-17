# from datetime import datetime
# from airflow import DAG
# from airflow.operators.postgres_operator import PostgresOperator

# dag_params = {
#     'dag_id': 'PostgresOperator_dag_10',
#     'start_date': datetime(2019, 10, 7),
#     'schedule_interval': None
# }

# with DAG(**dag_params) as dag:

#     create_table = PostgresOperator(
#         task_id='create_table',
#         postgres_conn_id="postgres_default",
#         sql='''CREATE TABLE new_table(
#             custom_id integer NOT NULL,
#             timestamp TIMESTAMP NOT NULL,
#             user_id VARCHAR (50) NOT NULL
#             );''',
#     )
#     insert_row = PostgresOperator(
#     task_id='insert_row',
#     postgres_conn_id="postgres_default",
#     sql='INSERT INTO new_table VALUES(%s, %s, %s)',
#     #trigger_rule=TriggerRule.ALL_DONE,
#     parameters=(uuid.uuid4().int % 123456789, datetime.now(), uuid.uuid4().hex[:10])
# )

# create_table >> insert_row