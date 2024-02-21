import sqlite3
import pandas as pd

def retrieve_table_name(path="E:\\Group2Networked_AI_Systems_Project\\models\\metric.db"):
    conn = sqlite3.connect(path)  # Replace 'your_database_name' with your actual database name
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    table_names = cursor.fetchall()
    conn.close()
    table_names = [name[0] for name in table_names]
    return table_names

def retrieve_by_pid(pid, path="E:\\Group2Networked_AI_Systems_Project\\models\\metric.db"):
    conn = sqlite3.connect(path)
    query = "SELECT * FROM 'merged metrics' WHERE PID = {pid}"
    result_df = pd.read_sql_query(query, conn)
    conn.close()
    return result_df

def retrieve_by_hthres(path="E:\\Group2Networked_AI_Systems_Project\\models\\metric.db"):
    conn = sqlite3.connect(path)
    query = "SELECT * FROM 'merged metrics' ORDER BY ht ASC"
    result_df = pd.read_sql_query(query, conn)
    conn.close()
    return result_df

def retrieve_by_lthres(path="E:\\Group2Networked_AI_Systems_Project\\models\\metric.db"):
    conn = sqlite3.connect(path)
    query = "SELECT * FROM 'merged metric's ORDER BY lt ASC"
    result_df = pd.read_sql_query(query, conn)
    conn.close()
    return result_df