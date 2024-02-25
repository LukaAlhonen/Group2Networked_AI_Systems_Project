"""
This is the enter point for the root cluster
"""
import sys
from schedule import schedule, task_creation
import tkinter as tk
import pandas as pd
import socket

if __name__ == "__main__":
    # Path of test file should be transferred by arguments in sys.argv

    args = sys.argv
    if len(args) != 2:
        raise ValueError("Please provide 1 absolute path of the batch!")
    
    path = args[1]
    df = pd.read_parquet(path)

    pids = schedule(df)

    if pids == "alarm":
        window = tk.Tk()
        window.title(f"Alarm")
        label = tk.Label(window, text="Alarm raised on the current batch", font=("Helvetica", 16))
        label.pack(pady=20)

        # Function to close the window
        def close_window():
            window.destroy()

        # Add a button to close the window
        button = tk.Button(window, text="Close", command=close_window)
        button.pack(pady=10)

        window.mainloop()
        sys.exit(-1)

    serialized_data = task_creation(pids, df)
# Ignore the following code, as they are generated for testing the worker
#    with open("data.pkl", "wb") as f:
#        f.write(serialized_data)

    # Socket Programming part - Transfer serialized_data from Root to Worker

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 8001))
    s.listen(5)

    while True:
        c, addr = s.accept()
        print(f'Connected to {addr}')
        c.send(serialized_data)
        c.close()
    