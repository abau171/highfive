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

    with m.run_task_set(LineTask(i) for i in range(10)) as task_set:
        for result in task_set.results():
            print(result)
    print("DONE 1")
    with m.run_task_set(LineTask(i) for i in range(10, 2000)) as task_set:
        for result in task_set.results():
            print(result)
            if result == "RESULT 20":
                task_set.cancel()
    print("DONE 2")
    with m.run_task_set(LineTask(i) for i in range(2000, 3000)) as task_set:
        print(task_set.next_result())
    print("DONE 3")
    with m.run_task_set(LineTask(i) for i in range(3000, 3010)) as task_set:
        for result in task_set.results():
            print(result)
    print("DONE 4")

except KeyboardInterrupt:

    print("cancelled")

