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
        read_file = connection.makefile("r")
        write_file = connection.makefile("w")
        write_file.write("task {}\n".format(self.i))
        write_file.flush()
        read_file.readline()
        self.done("RESULT {}".format(self.i))


try:

    m = master.Master("", 48484)

    for result in m.process(LineTask(i) for i in range(10)):
        print(result)
    print("DONE 1")
    for result in m.process(LineTask(i) for i in range(10, 2000)):
        print(result)
        if result == "RESULT 20":
            m.cancel_process()
    print("DONE 2")
    for result in m.process(LineTask(i) for i in range(2000, 2010)):
        print(result)
    print("DONE 3")

except KeyboardInterrupt:

    print("cancelled")

