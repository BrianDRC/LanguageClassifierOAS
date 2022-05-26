import utils
import sqlite3

def connect():
    return sqlite3.connect('database.db')

def disconnect(connection):
    connection.close()

def insert(connection, table, values: list):
    cur = connection.cursor()
    vals = ', '.join(values)
    
    query = f'INSERT INTO {table} VALUES ({vals})'
    cur.execute(query)
    connection.commit()

def update(connection, table, values: dict, conditions: dict):
    cur = connection.cursor()
    keys = list(values)
    vals = list(values.values())

    columns = ''
    for i in range(len(values)):
        columns += f"{keys[i]} = \"{vals[i]}\""
        if(i < (len(values) - 1)):
            columns += ', '

    keys = list(conditions)
    vals = list(conditions.values())
    conditions = ''
    for i in range(len(values)):
        conditions += f"{keys[i]} = \"{vals[i]}\""
        if(i < (len(values) - 1)):
            conditions += ' AND '

    query = f'UPDATE {table} SET {columns} WHERE {conditions}'
    cur.execute(query)
    connection.commit()

def searchInTable(connection, table, column, value):
    connection.row_factory = utils.dict_factory
    cur = connection.cursor()
    query = f'SELECT * FROM {table} WHERE {column}=\'{value}\''
    cur.execute(query)
    return cur.fetchone()

def createTable(connection, table, columns: list):
    cur = connection.cursor()
    cols = ', '.join(columns)
    query = f'CREATE TABLE {table} ({cols})'
    cur.execute(query)
    connection.commit()

def dropTable(connection, table):
    cur = connection.cursor()
    query = f'DROP TABLE {table}'
    cur.execute(query)
    connection.commit()

def truncateTable(connection, table):
    cur = connection.cursor()
    query = f'DELETE FROM {table}'
    cur.execute(query)
    connection.commit()
