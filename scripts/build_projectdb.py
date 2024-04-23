import psycopg2 as psql
from pprint import pprint
import os


# Read password from secrets file
file = os.path.join("secrets", ".psql.pass")

with open(file, "r") as file:
        password=file.read().rstrip()

# build connection string
conn_string="host=hadoop-04.uni.innopolis.ru port=5432 user=team29 dbname=team29_projectdb password={}".format(password)


# Connect to the remote dbms
with psql.connect(conn_string) as conn:
        
        # Create a cursor for executing psql commands
        cur = conn.cursor()
        # Read the commands from the file and execute them.

        print('Creating tables...')
        with open(os.path.join("sql","create_tables.sql")) as file:
                content = file.read()
                cur.execute(content)
        conn.commit()

        # Read the commands from the file and execute them.
        print('Importing data...')
        with open(os.path.join("sql", "import_data.sql")) as file:
                # We assume that the COPY commands in the file are ordered (1.city, 2.dataset)
                commands= file.readlines()
                with open(os.path.join("data","city.csv"), "r") as depts:
                        cur.copy_expert(commands[0], depts)
                with open(os.path.join("data","dataset.csv"), "r") as emps:
                        cur.copy_expert(commands[1], emps)

        # If the sql statements are CRUD then you need to commit the change
        conn.commit()

        pprint(conn)
        cur = conn.cursor()
        # Read the sql commands from the file
        print('Testing database...')
        with open(os.path.join("sql", "test_database.sql")) as file:
                commands = file.readlines()
                for command in commands:
                        cur.execute(command)
                        # Read all records and print them                        
                        pprint(cur.fetchall())