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
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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

    # Receive batch from root
    try:
        while True:
            c, addr = s.accept()
            print('Received batch.')
            chunks = b''
            i = 0
            while True:
                chunk = c.recv(1024)
                if not chunk:
                    break
                chunks += chunk
                i += 1
            print(f'Received {i} chunks')
            serialized_data = chunks
            deserialized_data = pickle.loads(serialized_data)
            pred, batch = task_process(deserialized_data)
            metrics = task_analysis(pred, batch)
            metrics['task id'] = deserialized_data['task id']
            
            print("<-------------------------------------------->")
            print(metrics)
            print("<-------------------------------------------->")
            print(pred)
            print("<-------------------------------------------->")

            # Get indexes of rows containing anomalies and send back to root
            anomalies = raise_alarm(pred=pred, thres=0.5)
            print('Sending anomaly indexes...')
            c.send(pickle.dumps(anomalies))
            print('Done.')
            c.close()
    except KeyboardInterrupt:
        s.close()