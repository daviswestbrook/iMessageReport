import sqlite3
import pandas as pd
USERNAME = daviswestbrook

def establish_connection():
    print("Connecting to database...")
    connection = sqlite3.connect('/Users/' + USERNAME + '/Library/Messages/chat.db')
    cursor = conn.cursor()
    return connection, cursor

def get_available_tables():
    connection, cursor = establish_connection()
    cursor.execute('select name from sqlite_master where type = "table"')
    table_list = []
    for name in cursor.fetchall():
        table_list.append(str(name))
    return table_list

def get_all_messages():
    connection, cursor = establish_connection()
    messages = pd.read_sql_query('select *, datetime(message.date/1000000000 + strftime("%s", "2001-01-01") ,"unixepoch","localtime") as date_uct from message', connection)
    return messages

def read_all_from_table(table):
    connection, cursor = establish_connection()
    table = pd.read_sql_query('select * from {}'.format(table), connection)

def read_all_from_table(table, condition):
    connection, cursor = establish_connection()
    table = pd.read_sql_query('select * from {} where {}'.format(table, condition), connection)
