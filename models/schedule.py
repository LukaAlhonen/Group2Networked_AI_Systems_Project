import pandas as pd
from similary_calculation import obtain_categorical_pattern, evaluate_categorical_similarity
from pipeline_metric_manage import retrieve_by_hthres, retrieve_by_lthres, retrieve_by_pid, retrieve_table_name

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