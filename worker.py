import socket
import json
import time

with socket.socket() as s:
    s.connect(("", 48484))
    sf = s.makefile("rw")
    while True:
        task = json.loads(sf.readline())
        a = task[0]
        b = task[1]
        print("calculation {} + {} ...".format(a, b))
        time.sleep(1)
        c = a + b
        print("result: {}".format(c))
        sf.write(json.dumps(c) + "\n")
        sf.flush()

