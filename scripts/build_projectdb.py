"""
This module implements methods that interact with a PostgreSQL database.
It creates the schema of tables 'city' and 'dataset', imports data into them,
and tests the database by selecting initial rows.
"""

import os
import psycopg2

# We should import specific functions directly
from pprint import pprint

# Constants should be named in all uppercase
PASSWORD_FILE_PATH = os.path.join("secrets", ".psql.pass")

def read_password(file_path):
    """
    Read and return the password from a file.

    Args:
        file_path (str): The file path to read the password from.

    Returns:
        str: The password.
    """
    with open(file_path, "r", encoding='utf-8') as file:
        return file.read().strip()

def create_connection(password):
    """
    Create and return a new database connection using the provided password.

    Args:
        password (str): The database password.

    Returns:
        connection: A new database connection.
    """
    conn_string = f"host=hadoop-04.uni.innopolis.ru port=5432 user=team29 \
                    dbname=team29_projectdb password={password}"
    return psycopg2.connect(conn_string)

def execute_sql_from_file(cursor, file_path):
    """
    Execute SQL commands from a file.

    Args:
        cursor (cursor): The database cursor.
        file_path (str): The path to the SQL file.
    """
    with open(file_path, encoding='utf-8') as file:
        content = file.read()
        cursor.execute(content)

def main():
    """
    Main function to create tables, import data, and test the database.
    """
    password = read_password(PASSWORD_FILE_PATH)
    with create_connection(password) as conn:
        cursor = conn.cursor()

        # Create tables
        print('Creating tables...')
        execute_sql_from_file(cursor, os.path.join("sql", "create_tables.sql"))
        conn.commit()

        # Import data
        print('Importing data...')
        # execute_sql_from_file(cursor, os.path.join("sql", "import_data.sql"))
        with open('./data/city.csv') as city:
            copy_sql = """
                        COPY city FROM stdin WITH CSV HEADER
                        DELIMITER ',' NULL AS 'NULL';
                        """
            cursor.copy_expert(sql=copy_sql, file=city)
        with open('./data/dataset.csv') as dataset:
            copy_sql = """
                        COPY dataset FROM stdin WITH CSV HEADER
                        DELIMITER ',' NULL AS 'NULL';
                        """
            cursor.copy_expert(sql=copy_sql, file=dataset)
        conn.commit()

        # Test the database
        print('Testing database...')
        execute_sql_from_file(cursor, os.path.join("sql", "test_database.sql"))
        # Fetch and print all records
        pprint(cursor.fetchall())

if __name__ == "__main__":
    main()