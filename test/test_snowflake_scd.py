import pytest
from unittest.mock import Mock
import sys
sys.path.append('/app/lib')
from postgres_loader_from_snowflake import SnowflakeSCD
from unittest.mock import patch
# Create mock objects for the SnowflakeHook and PostgresHook classes
sf_mock = Mock()
pg_mock = Mock()

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
        sf_mock.cursor.return_value.__enter__.return_value.execute.return_value = zip(sf_columns, sf_data_types)
        pg_mock.cursor.return_value.__enter__.return_value.execute.return_value = None

        # Act
        scd = SnowflakeSCD(sf_conn_id, pg_conn_id, sf_database, sf_schema)
        scd.create_table(table_name, sf_table)

        # Assert
        sf_mock.cursor.return_value.__enter__.return_value.execute.assert_called_once_with(
            "DESCRIBE {}.{}".format(sf_database, sf_table)
        )
        pg_mock.cursor.return_value.__enter__.return_value.execute.assert_called_once_with(
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

        sf_mock.cursor.return_value.__enter__.return_value.execute.return_value = zip(sf_columns, sf_data_types)
        pg_mock.cursor.return_value.__enter__.return_value.execute.return_value = None
        sf_mock.cursor.return_value.__enter__.return_value.fetchall.return_value = rows

        # Act
        scd = SnowflakeSCD(sf_conn_id, pg_conn_id, sf_database, sf_schema)
        scd.load_data(table_name, sf_table, scd_columns, batch_size)

        # Assert
        sf_mock.cursor.return_value.__enter__.return_value.execute.assert_called_once_with(
            "SELECT {} FROM {}.{}".format(", ".join(sf_columns), sf_database, sf_table)
        )
        pg_mock.cursor.return_value.__enter__.return_value.execute.assert_called_once_with(
            "INSERT INTO {} ({}) VALUES %s".format(table_name, ", ".join(sf_columns)),
            [(row,) for row in rows]
        )
        pg_mock.return_value.commit.assert_called()

        self.assertEqual(scd.load_data(table_name, sf_table, scd_columns, batch_size), len(rows))

    @pytest.fixture
    def sf_mock(self):
        with patch('airflow.providers.snowflake.hooks.snowflake.SnowflakeHook') as mock:
            yield mock

    @pytest.fixture
    def pg_mock(self):
        with patch('airflow.providers.postgres.hooks.postgres.PostgresHook') as mock:
            yield mock
