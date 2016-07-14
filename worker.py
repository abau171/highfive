import socket
import json
import time

with socket.socket() as s:
    s.connect(("", 48484))
    sf = s.makefile("rw")
    while True:
        task = json.loads(sf.readline())
        print(task)
        time.sleep(1)
        sf.write(json.dumps(task + 1) + "\n")
        sf.flush()
        print("done")

