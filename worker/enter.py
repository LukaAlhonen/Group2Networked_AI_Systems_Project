"""
Enter point for the worker.
"""

import pickle
import socket
import psutil
import os
import base64
import json
from dotenv import load_dotenv
from task import task_process, task_analysis, raise_alarm
from pipeline import Pipeline
from pipeline import Pipeline_info, get_lg10_for_duration, minmax_scaling_for_missing_bytes, \
                        bool_to_numeric, preprocessing_pipeline, transform_target, kn_pipeline,\
                        gb_pipeline, rf_pipeline, ridge_pipeline

def get_available_resources() -> tuple[str, str]:
    return (psutil.cpu_count(), psutil.virtual_memory().free)

if __name__ == "__main__":
    # Load env vars
    load_dotenv()
    root_address = os.environ['ROOT_ADDRESS']
    root_port = int(os.environ['ROOT_PORT'])
    worker_port = int(os.environ['WORKER_PORT'])

    # Setup sockets
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Socket for registering worker
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Socket for job and resource communcation
    s2.bind(('', worker_port))
    s1.connect((root_address, root_port))

    # Register worker
    message = {'type': 'worker_register', 'worker_port': worker_port} 
    s1.send(json.dumps(message).encode())
    s1.shutdown(socket.SHUT_WR)
    chunks = b''
    while True:
        chunk = s1.recv(1024)
        if not chunk:
            break
        chunks += chunk
    response = json.loads(chunks.decode())
    print(response)
    if response['type'] == 'worker_register':
        if response['success']:
            print('Successfully registered')
        else:
            print('Failed to register')

    s1.close()

    s2.listen()
    print(f'Listening on port {worker_port}')

    # Listen for messages from root
    try:
        while True:
            c, addr = s2.accept()
            chunks = b''
            while True:
                chunk = c.recv(1024)
                if not chunk:
                    break
                chunks += chunk
            message = json.loads(chunks.decode())

            # Send number of cpus and free memory to root
            if message['type'] == 'request_resources':
                cpus, memory = get_available_resources()
                response = {'cpus': cpus, 'memory': memory}
                c.send(json.dumps(response).encode())
                c.shutdown(socket.SHUT_WR)
            elif message['type'] == 'job':
                print(f'Received job')
                try: 
                    serialised_data = base64.b64decode(message['data'])
                    deserialized_data = pickle.loads(serialised_data)
                    if isinstance(deserialized_data, bytes):
                        deserialized_data = pickle.loads(deserialized_data)
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
                    serialised_anomalies = base64.b64encode(pickle.dumps(anomalies)).decode()
                    response = {'type': 'job', 'result': serialised_anomalies}
                    print('Sending anomaly indexes...')
                    c.send(json.dumps(response).encode())
                    print('Done.')
                    c.close()
                except KeyError:
                    print('Malformed traffic set')
                    c.send(json.dumps({'type': 'job', 'result': None}).encode())
                    c.close()
                    continue
    except KeyboardInterrupt:
        s2.close()