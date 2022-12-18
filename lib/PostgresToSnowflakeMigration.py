import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.snowflake_hook import SnowflakeHook


class PostgresToSnowflakeMigration:
    def __init__(self, postgres_conn_id, snowflake_conn_id, postgres_query, snowflake_table, batch_size=1000):
        self.postgres_conn_id = postgres_conn_id
        self.snowflake_conn_id = snowflake_conn_id
        self.postgres_query = postgres_query
        self.snowflake_table = snowflake_table
        self.batch_size = batch_size

    def create_table(self):
        postgres_hook = PostgresHook(self.postgres_conn_id)
        snowflake_hook = SnowflakeHook(self.snowflake_conn_id)

        # Get the metadata for the PostgreSQL table
        metadata = postgres_hook.get_table_metadata(self.postgres_query)
        columns = metadata['columns']
        column_str = ", ".join([f"{column['name']} {column['type']}" for column in columns])

        # Construct the CREATE TABLE statement for Snowflake
        create_table_stmt = f"CREATE TABLE {self.snowflake_table} ({column_str})"

        # Execute the CREATE TABLE statement
        snowflake_hook.run(create_table_stmt)

        logging.info(f"Created table {self.snowflake_table} in Snowflake")

    def execute(self, context):
        postgres_hook = PostgresHook(self.postgres_conn_id)
        snowflake_hook = SnowflakeHook(self.snowflake_conn_id)

        # Get the total number of rows in the PostgreSQL table
        row_count_query = f"SELECT COUNT(*) FROM ({self.postgres_query})"
        rows = postgres_hook.get_records(row_count_query)
        row_count = rows[0][0]

        # Calculate the number of batches needed to migrate all the data
        num_batches = row_count // self.batch_size
        if row_count % self.batch_size > 0:
            num_batches += 1

        # Migrate the data in batches
        for i in range(num_batches):
            # Calculate the OFFSET and LIMIT for the current batch
            offset = i * self.batch_size
            limit = self.batch_size

            # Construct the SELECT query for the current batch
            batch_query = f"SELECT * FROM ({self.postgres_query}) LIMIT {limit} OFFSET {offset}"

            # Execute the SELECT query and get the results
            rows = postgres_hook.get_records(batch_query)

            # Convert the results to a list of tuples
            tuples = [tuple(row) for row in rows]

            # Insert the tuples into the Snowflake table
            snowflake_hook.bulk_load(table=self.snowflake_table, tuples=tuples)

            logging.info(f"Migrated batch {i + 1} of {num_batches}")
