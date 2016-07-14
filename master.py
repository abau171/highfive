import socket
import time

import highfive


class AddTask(highfive.Task):

    def __init__(self, a, b):
        self._a = a
        self._b = b

    def prepare(self):
        return [self._a, self._b]

    def finish(self, result_struct):
        return self._a, self._b, result_struct


with highfive.Master() as m:

    ts1 = m.run_task_set(AddTask(i, i + 1) for i in range(10))
    ts2 = m.run_task_set(AddTask(i, i + 1) for i in range(10))

    for a, b, c in ts2.results():
        print("{} + {} = {}".format(a, b, c))
    for a, b, c in ts1.results():
        print("{} + {} = {}".format(a, b, c))

    print("closing...")
print("closed")

