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

if __name__ == "__main__":
    # Path of test file should be transferred by arguments in sys.argv

    args = sys.argv
    if 'batch_path' in os.environ:
        path = os.environ['batch_path']
    elif len(args) == 2:
        path = args[1]
    else:
        raise ValueError("Please provide 1 absolute path of the batch!")
    
    df = pd.read_parquet(path)

    pids = schedule(df)

    if pids == "alarm":
        # window = tk.Tk()
        # window.title(f"Alarm")
        # label = tk.Label(window, text="Alarm raised on the current batch", font=("Helvetica", 16))
        # label.pack(pady=20)

        # # Function to close the window
        # def close_window():
        #     window.destroy()

        # # Add a button to close the window
        # button = tk.Button(window, text="Close", command=close_window)
        # button.pack(pady=10)

        # window.mainloop()
        sys.exit(-1)

    serialized_data = task_creation(pids, df)
# Ignore the following code, as they are generated for testing the worker
#    with open("data.pkl", "wb") as f:
#        f.write(serialized_data)

    # Socket Programming part - Transfer serialized_data from Root to Worker

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    address = 'localhost' # Replace with root IP
    port = 8001
    s.connect((address, port))
    print(f'Connected to {address}:{port}')
    print('Sending serilazied data...')
    s.send(serialized_data)
    s.shutdown(socket.SHUT_WR)
    response = b''
    print('Waiting for response...')
    try:
        while True:
            chunk = s.recv(1024)
            if not chunk:
                break
            response += chunk
    except:
        print('Something unexpected occured...')
    deserialised_response = pickle.loads(response)
    print(deserialised_response)
    for idx in deserialised_response:
        pprint(df.iloc[idx].to_dict())
    print('Done.')
    s.close()
    
    