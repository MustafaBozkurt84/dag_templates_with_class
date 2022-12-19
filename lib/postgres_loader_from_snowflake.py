import psycopg2
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', filename='app.log')

class SnowflakeSCD:
    def __init__(self, sf_conn_id, pg_conn_id, sf_database, sf_schema):
        self.sf_conn_id = sf_conn_id
        self.pg_conn_id = pg_conn_id
        self.sf_database = sf_database
        self.sf_schema = sf_schema
        self.logger = logging.getLogger(__name__)

    def create_table(self, table_name, sf_table):
        # Get a connection to Snowflake using the provided Airflow connection ID
        self.logger.debug('Creating table')
        sf_hook = SnowflakeHook(self.sf_conn_id)
        sf_conn = sf_hook.get_conn()
        sf_cursor = sf_conn.cursor()

        # Get the column names and data types for the Snowflake table
        sf_cursor.execute("DESCRIBE {}.{}".format(self.sf_database, sf_table))
        sf_columns = [row[0] for row in sf_cursor]
        sf_data_types = [row[1] for row in sf_cursor]

        # Get a connection to PostgreSQL using the provided Airflow connection ID
        pg_hook = PostgresHook(self.pg_conn_id)
        pg_conn = pg_hook.get_conn()
        pg_cursor = pg_conn.cursor()

        # Generate the CREATE TABLE statement based on the Snowflake column names and data types
        create_stmt = "CREATE TABLE {} ({})".format(
            table_name,
            ", ".join(["{} {}".format(col, data_type) for col, data_type in zip(sf_columns, sf_data_types)])
        )

        # Execute the CREATE TABLE statement
        pg_cursor.execute(create_stmt)
        pg_conn.commit()
        pg_cursor.close()
        pg_conn.close()

    def load_data(self, table_name, sf_table, scd_columns, batch_size=1000):
        # Get a connection to Snowflake using the provided Airflow connection ID
        sf_hook = SnowflakeHook(self.sf_conn_id)
        sf_conn = sf_hook.get_conn()
        sf_cursor = sf_conn.cursor()

        # Get a connection to PostgreSQL using the provided Airflow connection ID
        pg_hook = PostgresHook(self.pg_conn_id)
        pg_conn = pg_hook.get_conn()
        pg_cursor = pg_conn.cursor()

        # Get the column names and data types for the Snowflake table
        sf_cursor.execute(f"DESCRIBE {self.sf_database}.{sf_table}")
        sf_columns = [row[0] for row in sf_cursor]
        sf_data_types = [row[1] for row in sf_cursor]

        # Get the row count of the Snowflake table
        sf_cursor.execute(f"SELECT COUNT(*) FROM {self.sf_database}.{sf_table}")
        sf_row_count = sf_cursor.fetchone()[0]

        # Get the row count of the PostgreSQL table
        pg_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        pg_row_count = pg_cursor.fetchone()[0]

        # Generate a SELECT statement to retrieve the data from the Snowflake table
        select_stmt = f"SELECT * FROM {self.sf_database}.{sf_table} LIMIT {batch_size} OFFSET {pg_row_count}"

        # Execute the SELECT statement and retrieve the data in batches
        while pg_row_count < sf_row_count:
            sf_cursor.execute(select_stmt)
            rows = sf_cursor.fetchmany(size=batch_size)

            # Filter out any rows that have already been loaded
            new_rows = []
            for row in rows:
                # Generate a SELECT statement to check if the row already exists in the PostgreSQL table
                select_stmt = f"SELECT * FROM {table_name} WHERE {' AND '.join([f'{col}='{row[i]}'' for i, col in enumerate(scd_columns)])}"
                pg_cursor.execute(select_stmt)
                if not pg_cursor.fetchone():
                    new_rows.append(row)

            # Insert the new rows into the PostgreSQL table

            insert_stmt = f"INSERT INTO {table_name} ({', '.join(sf_columns)}) VALUES ({', '.join(['%s' for _ in sf_columns])})"
            pg_cursor.executemany(insert_stmt, new_rows)
            pg_conn.commit()

            # Update the row count of the PostgreSQL table
            pg_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            pg_row_count = pg_cursor.fetchone()[0]

            # Generate a new SELECT statement with the updated offset
            select_stmt = f"SELECT * FROM {self.sf_database}.{sf_table} LIMIT {batch_size} OFFSET {pg_row_count}"

    def check_row_counts(self, table_name, sf_table):
        # Get a connection to Snowflake using the provided Airflow connection ID
        sf_hook = SnowflakeHook(self.sf_conn_id)
        sf_conn = sf_hook.get_conn()
        sf_cursor = sf_conn.cursor()

        # Get a connection to PostgreSQL using the provided Airflow connection ID
        pg_hook = PostgresHook(self.pg_conn_id)
        pg_conn = pg_hook.get_conn()
        pg_cursor = pg_conn.cursor()


        # Get the row count of the Snowflake table
        sf_cursor.execute(f"SELECT COUNT(*) FROM {self.sf_database}.{sf_table}")
        sf_row_count = sf_cursor.fetchone()[0]

        # Get the row count of the PostgreSQL table
        pg_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        pg_row_count = pg_cursor.fetchone()[0]

        # Print the row counts of the two tables

        self.logger.debug(f"Snowflake table '{sf_table}' has {sf_row_count} rows.")
        self.logger.debug(f"PostgreSQL table '{table_name}' has {pg_row_count} rows.")


