"""
This script contains several functions for managing metric.db which stores the metric used to evaluate each pipeline
The format of the table is as below:
-----------------------------------------------------------------------------
index: int | PID: str | Pattern: str | ht: float64 | lt: float64 | cid: int |
-----------------------------------------------------------------------------
"""

import sqlite3
import pandas as pd

# Replace this with the db in your own path
p = 'E:\\Group2Networked_AI_Systems_Project\\root\\metric.db' 

def retrieve_table_name(path=p):
    conn = sqlite3.connect(path)  # Replace 'your_database_name' with your actual database name
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    table_names = cursor.fetchall()
    conn.close()
    table_names = [name[0] for name in table_names]
    return table_names

def retrieve_by_pid(pid, path=p):
    conn = sqlite3.connect(path)
    query = "SELECT * FROM 'merged metrics' WHERE PID = {pid}"
    result_df = pd.read_sql_query(query, conn)
    conn.close()
    return result_df

def retrieve_by_hthres(path=p):
    conn = sqlite3.connect(path)
    query = "SELECT * FROM 'merged metrics' ORDER BY ht ASC"
    result_df = pd.read_sql_query(query, conn)
    conn.close()
    return result_df

def retrieve_by_lthres(path=p):
    conn = sqlite3.connect(path)
    query = "SELECT * FROM 'merged metrics' ORDER BY lt ASC"
    result_df = pd.read_sql_query(query, conn)
    conn.close()
    return result_df

if __name__ == '__main__':
    print(retrieve_by_hthres().head(2))