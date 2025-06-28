import os
import psycopg2
from psycopg2.extras import DictCursor, execute_values

class Database:
    def __init__(self):
        self.conn = None

    def connect(self):
        """Connect to the PostgreSQL database."""
        if self.conn is None or self.conn.closed:
            try:
                self.conn = psycopg2.connect(
                    host=os.getenv("POSTGRES_HOST"),
                    port=os.getenv("POSTGRES_PORT"),
                    dbname=os.getenv("POSTGRES_DB"),
                    user=os.getenv("POSTGRES_USER"),
                    password=os.getenv("POSTGRES_PASSWORD"),
                    cursor_factory=DictCursor,
                )
            except psycopg2.OperationalError as e:
                print(f"Error connecting to database: {e}")
                raise

    def disconnect(self):
        """Disconnect from the database."""
        if self.conn and not self.conn.closed:
            self.conn.close()

    def execute_query(self, query, params=None):
        """Execute a SQL query."""
        self.connect()
        with self.conn.cursor() as cursor:
            cursor.execute(query, params)
            self.conn.commit()
            try:
                return cursor.fetchall()
            except psycopg2.ProgrammingError:
                # No results to fetch
                return None

    def execute_batch(self, query, data_list):
        """Execute a batch of SQL queries for efficient inserts."""
        self.connect()
        with self.conn.cursor() as cursor:
            execute_values(cursor, query, data_list)
            self.conn.commit()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
