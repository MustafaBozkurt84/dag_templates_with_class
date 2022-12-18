import psycopg2
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.snowflake_hook import SnowflakeHook


class SnowflakeSCD:
    def __init__(self, sf_conn_id, pg_conn_id, sf_database, sf_schema):
        self.sf_conn_id = sf_conn_id
        self.pg_conn_id = pg_conn_id
        self.sf_database = sf_database
        self.sf_schema = sf_schema

    def create_table(self, table_name, sf_table):
        # Get a connection to Snowflake using the provided Airflow connection ID
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
        sf_cursor.execute("DESCRIBE {}.{}".format(self.sf_database, sf_table))
        sf_columns = [row[0] for row in sf_cursor]
        sf_data_types = [row[1] for row in sf_cursor]

        # Generate a SELECT statement to retrieve the data from the Snowflake table
        select_stmt = "SELECT {} FROM {}.{}".format(", ".join(sf_columns), self.sf_database, sf_table)

        # Execute the SELECT statement and retrieve the data in batches
        sf_cursor.execute(select_stmt)
        while True:
            rows = sf_cursor.fetchmany(size=batch_size)
            if not rows:
                break

            # Filter out any rows that have already been loaded
            new_rows = []
            for row in rows:
                # Generate a SELECT statement to check if the row already exists in the PostgreSQL table
                select_stmt = "SELECT * FROM {} WHERE {}".format(
                    table_name,
                    " AND ".join(["{}='{}'".format(col, row[i]) for i, col in enumerate(scd_columns)])
                )
                pg_cursor.execute(select_stmt)
                if not pg_cursor.fetchone():
                    new_rows.append(row)

            # Insert the new rows into the PostgreSQL table
            insert_stmt = "INSERT INTO {} ({}) VALUES ({})".format(
                table_name,
                ", ".join(sf_columns),
                ", ".join(["%s" for _ in sf_columns])
            )
            pg_cursor.executemany(insert_stmt, new_rows)
            pg_conn.commit()

        # Close the cursors and connections
        sf_cursor.close()
        sf_conn.close()
        pg_cursor.close()
        pg_conn.close()

