import socket
import time

import highfive


with highfive.Master("", 48484) as m:
    m.run_task_set(range(10))
    s1 = socket.socket()
    s1.connect(("", 48484))
    s2 = socket.socket()
    s2.connect(("", 48484))
    time.sleep(3)
    s1.close()
    s2.close()
    print("closing...")
print("closed")

