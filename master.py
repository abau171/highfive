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

    with m.run_task_set(AddTask(i, i + 1) for i in range(100)) as ts:
        for a, b, c in ts.results():
            print("{} + {} = {}".format(a, b, c))
            if a == 4:
                break

    with m.run_task_set(AddTask(i, i + 1) for i in range(10)) as ts:
        for a, b, c in ts.results():
            print("{} + {} = {}".format(a, b, c))

    print("closing...")
print("closed")

