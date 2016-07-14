import socket
import time

import highfive


with highfive.Master("", 48484) as m:
    ts1 = m.run_task_set(range(10))
    print(ts1)
    ts2 = m.run_task_set(range(10))
    print(ts2)
    s1 = socket.socket()
    s1.connect(("", 48484))
    time.sleep(0.1)
    s2 = socket.socket()
    s2.connect(("", 48484))
    time.sleep(2.7)
    s1.close()
    s2.close()
    print("closing...")
print("closed")

