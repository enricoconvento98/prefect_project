import pytest
from flows.example_flow import transform_data, load_data
from modules.database import Database
from unittest.mock import patch, MagicMock

# Import the db_connection fixture from tests/teFAILED flows/tests/test_example_flow.py::test_load_data - prefect.exceptions.MissingContextError: There is no active flow or task run context.st_database.py
pytest_plugins = ["tests.test_database"]

# def test_transform_data():
#     """Test the transform_data function to ensure it processes data correctly."""
#     # Sample input data
#     sample_data = [
#         {"id": 1, "value": 60},
#         {"id": 2, "value": 40},
#         {"id": 3, "value": 75},
#     ]
    
#     # Expected output after transformation
#     expected_output = [
#         {"id": 1, "value": 60, "transformed_value": 120, "category": "high"},
#         {"id": 2, "value": 40, "transformed_value": 80, "category": "low"},
#         {"id": 3, "value": 75, "transformed_value": 150, "category": "high"},
#     ]
    
#     # The transform_data function is a Prefect task, so we need to call .fn() 
#     # to get the raw Python function for testing.
#     result = transform_data.fn(sample_data)
    
#     assert result == expected_output

from flows.example_flow import _load_data_to_postgres
def test_load_data(db_connection):
    """Test the _load_data_to_postgres function to ensure it loads data into the database correctly."""
    test_table_name = "test_load_data_table"
    sample_data = [
        {"id": 101, "name": "item_A", "value": 10},
        {"id": 102, "name": "item_B", "value": 20},
    ]

    loaded_count = _load_data_to_postgres(sample_data, test_table_name)

    assert loaded_count == len(sample_data)

    # Verify data in the database
    with Database() as db:
        # Create table if it doesn't exist (load_data should handle this, but for direct query)
        columns = list(sample_data[0].keys())
        type_mapping = {
            int: "INTEGER",
            float: "REAL",
            str: "TEXT",
            bool: "BOOLEAN",
        }
        column_defs = [f'{col} {type_mapping.get(type(sample_data[0][col]), "TEXT")}' for col in columns]
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {test_table_name} ({', '.join(column_defs)});"
        db.execute_query(create_table_sql)

        result = db.execute_query(f"SELECT id, name, value FROM {test_table_name} ORDER BY id;")

        assert len(result) == len(sample_data)
        assert result[0]['id'] == sample_data[0]['id']
        assert result[0]['name'] == sample_data[0]['name']
        assert result[0]['value'] == sample_data[0]['value']
        assert result[1]['id'] == sample_data[1]['id']
        assert result[1]['name'] == sample_data[1]['name']
        assert result[1]['value'] == sample_data[1]['value']

        # Clean up the test table
        db.execute_query(f"DROP TABLE IF EXISTS {test_table_name};")
