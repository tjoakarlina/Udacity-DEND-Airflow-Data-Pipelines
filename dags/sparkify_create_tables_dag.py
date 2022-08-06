from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
import datetime

DAG_ID = "sparkify_create_tables_dag"
with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2018, 1, 1),
    schedule_interval='@once',
    catchup=False
) as dag:
    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="redshift",
        sql="sql/create_tables.sql"
    )
