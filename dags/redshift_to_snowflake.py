import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from RedshiftToSnowflakeMigration import RedshiftToSnowflakeMigration

from airflow.models import Variable

# Set the connection IDs for Redshift and Snowflake
Variable.set('redshift_conn_id', 'redshift_conn')
Variable.set('snowflake_conn_id', 'snowflake_conn')

# Set the Redshift query and the name of the Snowflake table
Variable.set('redshift_query', 'SELECT * FROM users')
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
    'redshift_to_snowflake_migration',
    default_args=default_args,
    schedule_interval=timedelta(hours=1)
)

# Load the connection IDs for Redshift and Snowflake from Airflow Variables
redshift_conn_id = Variable.get('redshift_conn_id')
snowflake_conn_id = Variable.get('snowflake_conn_id')

# Load the Redshift query and the name of the Snowflake table from Airflow Variables
redshift_query = Variable.get('redshift_query')
snowflake_table = Variable.get('snowflake_table')

# Create an instance of the RedshiftToSnowflakeMigration class
migration = RedshiftToSnowflakeMigration(redshift_conn_id, snowflake_conn_id, redshift_query, snowflake_table)

# Define a PythonOperator to create the Snowflake table
create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=migration.create_table,
    dag=dag
)

# Define a PythonOperator to migrate the data from Redshift to Snowflake
migrate_data_task = PythonOperator(
    task_id='migrate_data',
    python_callable=migration.execute,
    provide_context=True,
    dag=dag
)

# Set the dependencies for the tasks
create_table_task >> migrate_data_task
