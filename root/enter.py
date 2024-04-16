"""
This is the enter point for the root cluster
"""
import sys
import os
from schedule import schedule, task_creation
import tkinter as tk
import pandas as pd
import socket
import pickle
from pprint import pprint
import json
import threading
from dotenv import load_dotenv
import time
import random
import base64

class Worker:
    def __init__(self, address: str, port: int):
        self.address = address
        self.port = port
        self.is_busy = False

# List of worker nodes
workers = []

lock = threading.Lock()

def get_worker_resources(worker: Worker) -> tuple[str, str]:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((worker.address, worker.port))
    message = {'type': 'request_resources'}
    s.send(json.dumps(message).encode())
    s.shutdown(socket.SHUT_WR)
    s.settimeout(60)
    try:
        chunks = b''
        while True:
            chunk = s.recv(1024)
            if not chunk:
                break
            chunks += chunk
        resources = json.loads(chunks.decode())
        s.close()
        return (resources['cpus'], resources['memory'])
    except socket.timeout:
        print(f'Worker {worker.address} timed out')

def get_eligible_workers() -> list[Worker]:
    # Get all registered workers that are not busy
    global workers
    with lock:
        eligible_workers = []
        for worker in workers:
            if not worker.is_busy:
                eligible_workers.append(worker)
            else:
                print(f'Worker {worker.address} is busy')

    return eligible_workers

def send_job(job, worker: Worker) -> any:
    # Send job to worker and get result of job
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    response = ''
    print(f'Sending job to worker {worker.address}')
    s.connect((worker.address, worker.port))
    serialised_data = base64.b64encode(pickle.dumps(job)).decode()
    message = {'type': 'job', 'data': serialised_data}
    s.send(json.dumps(message).encode())
    s.shutdown(socket.SHUT_WR)
    worker.is_busy = True
    s.settimeout(600)
    try:
        chunks = b''
        while True:
            chunk = s.recv(1024)
            if not chunk:
                break
            chunks += chunk
        response = json.loads(chunks.decode())
        worker.is_busy = False
        if response['type'] == 'job':
            if response['result'] == None:
                return None
            serialised_response = base64.b64decode(response['result'])
            deserialised_data = pickle.loads(serialised_response)
            return deserialised_data
    except:
        print('Something went wrong')
    s.close()

    return None


def schedule_job(job: any) -> any:
    response = None
    eligible_workers = get_eligible_workers()
    target_worker = None
    target_cpus = 0
    target_memory = 0

    # get worker with most resources
    if len(eligible_workers) > 0:
        for worker in eligible_workers:
            cpus, memory = get_worker_resources(worker)
            print(f'Worker {worker.address} has {cpus} cpus and {memory} MB free memory')
            if cpus >= target_cpus and memory >= target_memory:
                target_cpus = cpus
                target_memory = memory
                target_worker = worker
        response = send_job(job, target_worker)
    return response

def handle_worker_register(worker_address: str, worker_port: int) -> bool:
    isregistered = False
    new_worker = Worker(worker_address, int(worker_port))
    success = False
    is_local = None
    try:
        is_local = bool(sys.argv[1])
    except:
        is_local = False
    global workers
    with lock:
        if not is_local:
            if len(workers) > 0:
                for worker in workers:
                    if worker.address == worker_address:
                        isregistered = True
            if not isregistered:
                workers.append(new_worker)
                success = True
        else:
            workers.append(new_worker)
            success = True

    return success

def listen_for_registration(s: socket, stop_event: threading.Event) -> None:
    s.settimeout(10) # This is set to properly handle KeyboardInterrupt
    while not stop_event.is_set():
        try:
            c, addr = s.accept()
            chunks = b''
            while True:
                chunk = c.recv(1024)
                if not chunk:
                    break
                chunks += chunk
            message = json.loads(chunks.decode())
            if message['type'] == 'worker_register':
                worker_address = addr[0]
                worker_port = message['worker_port']
                if handle_worker_register(worker_address, worker_port):
                    print(f'Worker {worker_address} successfully registered')
                    response = {'type': 'worker_register', 'success': True}
                else:
                    response = {'type': 'worker_register', 'success': False}
                c.send(json.dumps(response).encode())
                c.shutdown(socket.SHUT_WR)
                
        except socket.timeout:
            continue

def get_random_traffic_set():
    dir = 'test_set'
    files = os.listdir(dir)
    random_file = random.choice(files)
    return os.path.join(dir, random_file)

def get_job():
    path = get_random_traffic_set()
    df = pd.read_parquet(path)
    pids = schedule(df)
    return (pids, df)

if __name__ == "__main__":
    load_dotenv() # Load env vars

    try: 
        # Start listening
        port = int(os.environ['ROOT_PORT'])
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('', port))
        s.listen(1)
        print(f'Listening on port: {port}')
        stop_event = threading.Event()
        listen_thread = threading.Thread(target=listen_for_registration, args=(s,stop_event))
        listen_thread.start()

        # Send a random job to a worker every 10 seconds
        while True:
            if len(workers) > 0:
                print('Scheduling job')
                pids, df = get_job()
                if pids == 'alarm':
                    continue
                serialized_data = task_creation(pids, df)
                result = schedule_job(serialized_data)
                if not result is None:
                    print(result)
                    for idx in result:
                        pprint(df.iloc[idx].to_dict())
                    print('Done.')
            time.sleep(1)


    except KeyboardInterrupt:
        print('\nExiting...')
        stop_event.set()
        listen_thread.join()
        s.close()
        sys.exit(1)
    