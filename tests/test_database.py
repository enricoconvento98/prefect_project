import pytest
from modules.database import Database
import os

# Pytest fixture to set up and tear down the test table
@pytest.fixture(scope="module")
def db_connection():
    """Set up a database connection and a test table for the tests."""
    db = Database()
    db.connect()
    
    # Create a test table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS test_table (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        value INTEGER
    );
    """
    db.execute_query(create_table_query)
    
    yield db
    
    # Teardown: drop the test table
    drop_table_query = "DROP TABLE IF EXISTS test_table;"
    db.execute_query(drop_table_query)
    db.disconnect()

def test_insert_and_select_data(db_connection):
    """Test inserting data into the test table and then selecting it."""
    # Insert test data
    insert_query = "INSERT INTO test_table (name, value) VALUES (%s, %s);"
    db_connection.execute_query(insert_query, ("test_name", 123))
    
    # Select the data back
    select_query = "SELECT name, value FROM test_table WHERE name = 'test_name';"
    result = db_connection.execute_query(select_query)
    
    assert len(result) == 1
    assert result[0]['name'] == "test_name"
    assert result[0]['value'] == 123

def test_context_manager(db_connection):
    """Test that the database connection is opened and closed with a context manager."""
    with Database() as db:
        assert db.conn is not None
        assert not db.conn.closed
    
    assert db.conn.closed

