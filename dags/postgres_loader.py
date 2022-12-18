import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.snowflake_hook import SnowflakeHook
from airflow.models import Variable
from lib.SnowflakeToPostgres import LoadDataOperator
# Define default_args dictionary to specify default parameters of the DAG, such as the start date, frequency, etc.
default_args = {
    'owner': 'me',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the list of tables to load data into as a JSON-serialized list
tables_var = Variable(key='tables', value='["table1", "table2", "table3"]', serialize_json=True)
tables_var.save()

# Define the SCD columns for each table as JSON-serialized lists
table1_scd_columns_var = Variable(key='table1_scd_columns', value='["col1", "col2"]', serialize_json=True)
table1_scd_columns_var.save()

table2_scd_columns_var = Variable(key='table2_scd_columns', value='["col3", "col4"]', serialize_json=True)
table2_scd_columns_var.save()

table3_scd_columns_var = Variable(key='table3_scd_columns', value='["col5", "col6"]', serialize_json=True)
table3_scd_columns_var.save()


# Create a DAG instance and pass it the default_args dictionary
dag = DAG(
    'load_data_dag',
    default_args=default_args,
    description='Load data from Snowflake to Postgres using batches and slowly changing dimensions',
    schedule_interval=timedelta(hours=1),
)
# Load the list of tables to load data into from a variable
tables = Variable.get('tables', deserialize_json=True)

# Iterate through the list of tables and create a task for each table
for table in tables:
    # Load the Snowflake connection ID, Postgres connection ID, SQL query, and SCD columns from variables
    snowflake_conn_id = Variable.get(f"{table}_snowflake_conn_id")
    postgres_conn_id = Variable.get(f"{table}_postgres_conn_id")
    sql = Variable.get(f"{table}_sql")
    scd_columns = Variable.get(f"{table}_scd_columns", deserialize_json=True)

    task = LoadDataOperator(
        task_id=f"load_{table}",
        snowflake_conn_id=snowflake_conn_id,
        postgres_conn_id=postgres_conn_id,
        sql=sql,
        scd_columns=scd_columns,
        table=table,
        dag=dag,
    )

# Set the dependencies between the tasks
for i, table in enumerate(tables):
    if i > 0:
        previous_task = f"load_{tables[i-1]}"
        task.set_upstream(previous_task)