import time

import worker


class LineWorker(worker.Worker):

    def run(self, connection):
        read_file = connection.makefile("r")
        write_file = connection.makefile("w")
        while True:
            print(read_file.readline().rstrip("\n"))
            time.sleep(1)
            write_file.write("mmk\n")
            write_file.flush()


try:
    worker.run_workers("", 48484, LineWorker())
except KeyboardInterrupt:
    print("cancelled")

