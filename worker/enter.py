"""
Enter point for the worker.
"""

import pickle
import socket
import os
from task import task_process, task_analysis, raise_alarm
from pipeline import Pipeline
from pipeline import Pipeline_info, get_lg10_for_duration, minmax_scaling_for_missing_bytes, \
                        bool_to_numeric, preprocessing_pipeline, transform_target, kn_pipeline,\
                        gb_pipeline, rf_pipeline, ridge_pipeline

if __name__ == "__main__":
    # Socket Programming part, receive serialized_data from Root
    #   ....
    #   serialized_data = Socket.receive()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #address = os.environ['address'] # Replace with root IP
    #port = int(os.environ['port'])
    try:
        if 'address' not in os.environ and 'port' not in os.environ:
            print('No address or port specified, binding to "localhost:8001"')
            address = 'localhost'
            port = 8001
        else:
            address = os.environ['address']
            port = int(os.environ['port'])
    except ValueError as e:
        print(f'Invalid port: {e}')
    s.bind((address, port))
    s.listen(1)
    print('Listening...')
    try:
        while True:
            c, addr = s.accept()
            print('Received batch.')
            chunks = b''
            i = 0
            while True:
                # print(f'Chunk: {i}')
                chunk = c.recv(1024)
                if not chunk:
                    break
                chunks += chunk
                i += 1
            print(f'Received {i} chunks')
            serialized_data = chunks

            # with open("E:\\Group2Networked_AI_Systems_Project\\worker\\data.pkl", "rb") as f:
            #     serialized_data = f.read()
            deserialized_data = pickle.loads(serialized_data)
            #print(deserialized_data)
            pred, batch = task_process(deserialized_data)
            metrics = task_analysis(pred, batch)
            metrics['task id'] = deserialized_data['task id']
            
            print("<-------------------------------------------->")
            print(metrics)
            print("<-------------------------------------------->")
            print(pred)
            print("<-------------------------------------------->")
            anomalies = raise_alarm(pred=pred, thres=0.5)
            print(anomalies)
            print('Sending anomaly indexes...')
            c.send(pickle.dumps(anomalies))
            print('Done.')
            c.close()
    except KeyboardInterrupt:
        s.close()

    # with open("E:\\Group2Networked_AI_Systems_Project\\worker\\data.pkl", "rb") as f:
    #     serialized_data = f.read()
    # deserialized_data = pickle.loads(serialized_data)
    #print(deserialized_data)
    # pred, batch = task_process(deserialized_data)
    # metrics = task_analysis(pred, batch)
    # metrics['task id'] = deserialized_data['task id']
    # for a in pred:
    #     t_s = 0
    #     f_s = 0
    #     for elem in a:
    #         if elem: t_s += 1
    #         else: f_s += 1
    #     print(f'True: {t_s}')
    #     print(f'False: {f_s}')
    # print("<-------------------------------------------->")
    # print(metrics)
    # print("<-------------------------------------------->")
    # print(pred)
    # 