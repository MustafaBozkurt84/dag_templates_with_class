import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from PostgresToSnowflakeMigration import PostgresToSnowflakeMigration

# Set the connection IDs for PostgreSQL and Snowflake
Variable.set('postgres_conn_id', 'postgres_conn')
Variable.set('snowflake_conn_id', 'snowflake_conn')

# Set the PostgreSQL query and the name of the Snowflake table
Variable.set('postgres_query', 'SELECT * FROM users')
Variable.set('snowflake_table', 'users')

# Set default_args for the DAG
default_args = {
    'owner': 'me',
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create the DAG
dag = DAG(
    'postgres_to_snowflake_migration',
    default_args=default_args,
    schedule_interval=timedelta(hours=1)
)

# Load the connection IDs for PostgreSQL and Snowflake from Airflow Variables
postgres_conn_id = Variable.get('postgres_conn_id')
snowflake_conn_id = Variable.get('snowflake_conn_id')

# Load the PostgreSQL query and the name of the Snowflake table from Airflow Variables
postgres_query = Variable.get('postgres_query')
snowflake_table = Variable.get('snowflake_table')

# Create an instance of the PostgresToSnowflakeMigration class
migration = PostgresToSnowflakeMigration(postgres_conn_id, snowflake_conn_id, postgres_query, snowflake_table)

# Define a PythonOperator to create the Snowflake table
create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=migration.create_table,
    dag=dag
)

# Define a PythonOperator to migrate the data from PostgreSQL to Snowflake
migrate_data_task = PythonOperator(
    task_id='migrate_data',
    python_callable=migration.execute,
    provide_context=True,
    dag=dag
)

# Set the dependencies for the tasks
create_table_task >> migrate_data_task
