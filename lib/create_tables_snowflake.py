import airflow.hooks.snowflake_hook

class SnowflakeTableCreator:
    def __init__(self, snowflake_conn_id):
        self.snowflake_conn_id = snowflake_conn_id

    def create_table(self, postgres_table, snowflake_table):
        # Get the Snowflake hook
        snowflake_hook = airflow.hooks.snowflake_hook.SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)

        # Get column names and data types from the PostgreSQL table
        postgres_hook = airflow.hooks.postgres_hook.PostgresHook()
        rows = postgres_hook.get_records(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{postgres_table}'")
        column_defs = []
        for row in rows:
            column_name = row[0]
            data_type = row[1]
            column_def = f"{column_name} {data_type}"
            column_defs.append(column_def)

        # Generate the CREATE TABLE statement for Snowflake
        create_table_stmt = f"CREATE TABLE {snowflake_table} ({', '.join(column_defs)})"

        # Execute the CREATE TABLE statement in Snowflake
        snowflake_hook.run(create_table_stmt)
        print(f"Successfully created table {snowflake_table} in Snowflake!")
