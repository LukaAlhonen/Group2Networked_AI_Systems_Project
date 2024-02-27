"""
Enter point for the worker.
"""

import pickle
import socket
from task import task_process, task_analysis
from pipeline import Pipeline
from pipeline import Pipeline_info, get_lg10_for_duration, minmax_scaling_for_missing_bytes, \
                        bool_to_numeric, preprocessing_pipeline, transform_target, kn_pipeline,\
                        gb_pipeline, rf_pipeline, ridge_pipeline

if __name__ == "__main__":
    # Socket Programming part, receive serialized_data from Root
    #   ....
    #   serialized_data = Socket.receive()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    address = 'localhost' # Replace with root IP
    port = 8001
    s.connect((address, port))
    chunks = []
    while True:
        chunk = s.recv(1024)
        if not chunk:
            break
        chunks.append(chunk)
    serialized_data = b''.join(chunks)
    s.close()

    # with open("E:\\Group2Networked_AI_Systems_Project\\worker\\data.pkl", "rb") as f:
    #     serialized_data = f.read()
    deserialized_data = pickle.loads(serialized_data)
    #print(deserialized_data)
    pred, batch = task_process(deserialized_data)
    metrics = task_analysis(pred, batch)
    metrics['task id'] = deserialized_data['task id']
    for a in pred:
        t_s = 0
        f_s = 0
        for elem in a:
            if elem: t_s += 1
            else: f_s += 1
        print(f'True: {t_s}')
        print(f'False: {f_s}')
    print("<-------------------------------------------->")
    print(metrics)
    print("<-------------------------------------------->")
    print(pred)
    # 