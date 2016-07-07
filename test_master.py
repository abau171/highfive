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

    # normal process
    p1 =  m.process(LineTask(i) for i in range(10))
    for result in p1.results():
        print(result)
    print("DONE 1")

    # early close
    p2 = m.process(LineTask(i) for i in range(10, 2000))
    for result in p2.results():
        print(result)
        if result == "RESULT 20":
            p2.close()
    print("DONE 2")

    # first result close
    p3 =  m.process(LineTask(i) for i in range(2000, 3000))
    print(next(p3.results()))
    p3.close()
    print("DONE 3")

    # out-of-order results
    p4 =  m.process(LineTask(i) for i in range(3000, 3010))
    p5 =  m.process(LineTask(i) for i in range(3010, 3020))
    for result in p5.results():
        print(result)
    print("DONE 5")
    for result in p4.results():
        print(result)
    print("DONE 4")

except KeyboardInterrupt:

    print("cancelled")

