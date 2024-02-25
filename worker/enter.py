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
    address = '192.168.1.36' # Replace with root IP
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
    print(deserialized_data)
    pred, batch = task_process(deserialized_data)
    metrics = task_analysis(pred, batch)
    metrics['task id'] = deserialized_data['task id']
    
    # 