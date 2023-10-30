from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import BaseOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults
import random
import string
import psycopg2.extras

class GenerateAndInsertRandomData(BaseOperator):

    @apply_defaults
    def __init__(self, postgres_conn_id, table_name, column_name, *args, **kwargs):
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name
        self.column_name = column_name
        super().__init__(*args, **kwargs)

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()

        # Drop table if it exists and recreate
        drop_create_query = f"""
        DROP TABLE IF EXISTS {self.table_name};
        CREATE TABLE {self.table_name} ({self.column_name} varchar(5));
        """
        cur.execute(drop_create_query)

        # Generate random data
        data = [(random_string(), ) for _ in range(1000)]  # assuming 1000 rows, adjust as necessary        

        insert_query = f"INSERT INTO {self.table_name} ({self.column_name}) VALUES %s"
        psycopg2.extras.execute_values(cur, insert_query, data, template=None, page_size=100)  # page size can be adjusted to control memory usage and speed

        conn.commit()
        cur.close()
        conn.close()

def random_string():
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(5))

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
}

dag = DAG(
    'postgres_insert_random_bulk',
    default_args=default_args,
    description='A simple DAG to insert random data into PostgreSQL in bulk',
    schedule_interval='@daily'
)

with dag:
    generate_and_insert_data = GenerateAndInsertRandomData(
        task_id='generate_and_insert_data',
        postgres_conn_id='postgres_default',
        table_name='my_table_name',
        column_name='my_column_name',
    )

    generate_and_insert_data
