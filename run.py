import socket
import time

import highfive


with highfive.Master("", 48484) as m:

    ts1 = m.run_task_set(range(10))
    ts2 = m.run_task_set(range(10))

    s1 = socket.socket()
    s2 = socket.socket()

    s1.connect(("", 48484))
    s2.connect(("", 48484))


    for result in ts1.results():
        print(result)
    for result in ts2.results():
        print(result)
    for result in ts1.results():
        print(result)

    s1.close()
    s2.close()

    print("closing...")
print("closed")

