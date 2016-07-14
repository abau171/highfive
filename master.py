import socket
import time

import highfive


with highfive.Master("", 48484) as m:

    ts1 = m.run_task_set(range(10))
    ts2 = m.run_task_set(range(10))

    for result in ts1.results():
        print(result)
    for result in ts2.results():
        print(result)
    for result in ts1.results():
        print(result)

    print("closing...")
print("closed")

