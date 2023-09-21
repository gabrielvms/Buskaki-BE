import psycopg2
from psycopg2 import Error
import json
import os

# Read database connection information and create a dictionary from it
with open("db_info.json", 'r') as db_info_file:
    db_info = json.load(db_info_file)

# Open a database connection
def open_connection():
    con = None
    try:
        con = psycopg2.connect(
            host=os.environ.get('POSTGRES_HOST'),
            port=os.environ.get('POSTGRES_PORT'),
            database=os.environ.get('POSTGRES_DATABASE'),
            user=os.environ.get('POSTGRES_USER'),
            password=os.environ.get('POSTGRES_PASSWORD')
        )
    except Error as e:
        print(e)
    finally:
        return con, con.cursor()
    
def close_connection(cursor, connection):
    try:
        cursor.close()
        connection.close()
    except Error as e:
        print(e)
    finally:
        print("connection closed")


def companies_read():
    con, cur = open_connection()
    cur.execute("select * from buskaki_estabelecimentos")
    column_names = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    companies =  [row_to_dict(row, column_names) for row in rows] 
    con.commit()
    con.close()
    return companies

# Define a helper function to convert a row to a dictionary
def row_to_dict(row, column_names):
    return dict(zip(column_names, row))