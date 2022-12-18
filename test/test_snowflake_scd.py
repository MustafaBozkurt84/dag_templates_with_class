import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import sys
sys.path.append('/lib')
from postgres_loader_from_snowflake import SnowflakeSCD
from unittest.mock import patch
'''This test class includes two test methods: `test_create_table` and `test_load_data`. 
The `test_create_table` method tests the `create_table` method of the `SnowflakeSCD` class, 
while the `test_load_data` method tests the `load_data` method.

The test class also includes two fixtures: `sf_mock` and `pg_mock`, 
which are used to mock the `SnowflakeHook` and `PostgresHook` classes, respectively. 
These fixtures allow you to mock the methods and properties of these classes so that 
you can test the `SnowflakeSCD` class in isolation.'''

@pytest.mark.usefixtures('sf_mock', 'pg_mock')
class TestSnowflakeSCD:
    def test_create_table(self, sf_mock, pg_mock):
        # Arrange
        sf_conn_id = 'sf_conn_id'
        pg_conn_id = 'pg_conn_id'
        sf_database = 'sf_database'
        sf_schema = 'sf_schema'
        table_name = 'table_name'
        sf_table = 'sf_table'
        sf_columns = ['col1', 'col2', 'col3']
        sf_data_types = ['varchar', 'int', 'timestamp']
        sf_mock.return_value.cursor.return_value.__enter__.return_value.execute.return_value = zip(sf_columns,
                                                                                                   sf_data_types)
        pg_mock.return_value.cursor.return_value.__enter__.return_value.execute.return_value = None

        # Act
        scd = SnowflakeSCD(sf_conn_id, pg_conn_id, sf_database, sf_schema)
        scd.create_table(table_name, sf_table)

        # Assert
        sf_mock.return_value.cursor.return_value.__enter__.return_value.execute.assert_called_once_with(
            "DESCRIBE {}.{}".format(sf_database, sf_table)
        )
        pg_mock.return_value.cursor.return_value.__enter__.return_value.execute.assert_called_once_with(
            "CREATE TABLE {} ({})".format(
                table_name,
                ", ".join(["{} {}".format(col, data_type) for col, data_type in zip(sf_columns, sf_data_types)])
            )
        )

    def test_load_data(self, sf_mock, pg_mock):
        # Arrange
        sf_conn_id = 'sf_conn_id'
        pg_conn_id = 'pg_conn_id'
        sf_database = 'sf_database'
        sf_schema = 'sf_schema'
        table_name = 'table_name'
        sf_table = 'sf_table'
        scd_columns = ['col1', 'col2']
        batch_size = 1000
        sf_columns = ['col1', 'col2', 'col3']
        sf_data_types = ['varchar', 'int', 'timestamp']
        rows = [['a', 1, '2022-01-01'], ['b', 2, '2022-01-02'], ['c', 3, '2022-01-03']]

        sf_mock.return_value.cursor.return_value.__enter__.return_value.execute.side_effect = [
            zip(sf_columns, sf_data_types), rows]
        pg_mock.return_value.cursor.return_value.__enter__.return_value.execute.side_effect = [None, None]
        pg_mock.return_value.cursor.return_value.__enter__.return_value.fetchone.return_value = None

        # Act
        scd = SnowflakeSCD(sf_conn_id, pg_conn_id, sf_database, sf_schema)
        scd.load_data(table_name, sf_table, scd_columns, batch_size=batch_size)

        # Assert
        sf_mock.return_value.cursor.return_value.__enter__.return_value.execute.assert_called_with(
            "SELECT {} FROM {}.{}".format(", ".join(sf_columns), sf_database, sf_table)
        )
        assert sf_mock.return_value.cursor.return_value.__enter__.return_value.execute.call_count == 2
        pg_mock.return_value.cursor.return_value.__enter__.return_value.execute.assert_called_with(
            "INSERT INTO {} ({}) VALUES ({})".format(
                table_name,
                ", ".join(sf_columns),
                ", ".join(["%s"] * len(sf_columns))
            ),
            rows[0]
        )
        assert pg_mock.return_value.cursor.return_value.__enter__.return_value.execute.call_count == 2

    @pytest.fixture
    def sf_mock(self):
        with patch('airflow.providers.snowflake.hooks.snowflake') as mock:
            yield mock

    @pytest.fixture
    def pg_mock(self):
        with patch('airflow.providers.postgres.hooks.postgres') as mock:
            yield mock

'''sf_mock.return_value.cursor.return_value.__enter__.return_value.execute.side_effect = [zip(sf_columns, sf_data_types), rows]: 
This line of code is setting the side_effect attribute of the execute method of the SnowflakeHook class to 
a list containing two elements: zip(sf_columns, sf_data_types) and rows. 
The side_effect attribute is used to specify the return value or the exception that should be raised when the method is called. 
In this case, the execute method will return zip(sf_columns, sf_data_types) the first time it is called, 
and rows the second time it is called.

pg_mock.return_value.cursor.return_value.__enter__.return_value.execute.side_effect = [None, None]: 
This line of code is setting the side_effect attribute of the execute method of the PostgresHook class to a 
list containing two None values. The side_effect attribute is used to specify the return value or the exception 
that should be raised when the method is called. In this case, the execute method will return None each time it is called.

pg_mock.return_value.cursor.return_value.__enter__.return_value.fetchone.return_value = None: 
This line of code is setting the return_value attribute of the fetchone method of the 
PostgresHook class to None. The return_value attribute is used to specify the return value that should 
be returned when the method is called. In this case, the fetchone method will return None each time it is called.'''