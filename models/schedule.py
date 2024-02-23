import pandas as pd
from similary_calculation import obtain_categorical_pattern, evaluate_categorical_similarity
from pipeline_metric_manage import retrieve_by_hthres, retrieve_by_lthres, retrieve_by_pid, retrieve_table_name
import uuid
import random

def schedule(df: pd.DataFrame):
    # return a list of PIDs
    new_pattern = obtain_categorical_pattern(df)
    patterns = retrieve_by_hthres()
    hpids = []
    lpids = []
    for _, record in patterns.iterrows():
        sim = evaluate_categorical_similarity(new_pattern, record['Pattern'])
        if sim >= record['ht'] and len(hpids) < 3:
            hpids.append(record['PID'])
        if sim >= record['lt'] and len(lpids) < 7:
            lpids.append(record['PID'])
    return hpids, lpids

def generate_short_task_id(length=8):
    # Generate a UUID
    uuid_str = str(uuid.uuid4())

    # Remove hyphens from the UUID
    uuid_str = uuid_str.replace('-', '')

    # Extract a random substring from the UUID
    start_index = random.randint(0, len(uuid_str) - length)
    short_task_id = uuid_str[start_index:start_index+length]

    return short_task_id

def task_creation(hpids, lpids, df):
    # Create tasks: [task_id, batch, CID, [Pid1, Pid2, ...]]
    tid = generate_short_task_id()
    batch = df
    cid = 1
    pid = lpids + hpids
    alarm = False
    task = [tid, batch, cid, pid, alarm]
    # whether to store the task into local memory is to be discussed
    return task