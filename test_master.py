import time
import random

import master
import task_manager


class LineTask(task_manager.Task):

    def __init__(self, i):
        super().__init__()
        self.i = i

    def run(self, connection):
        time.sleep(0.2)
        if random.randint(0, 1) == 0:
            print("simulated failure on task {}".format(self.i))
            return
        write_file = connection.makefile("w")
        write_file.write("task {}\n".format(self.i))
        self.done()


m = master.Master("", 48484)


m.process(LineTask(i) for i in range(10))
print("DONE 1")
m.process(LineTask(i) for i in range(10, 20))
print("DONE 2")

