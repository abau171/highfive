import time

import worker


class LineWorker(worker.Worker):

    def run(self, connection):
        read_file = connection.makefile("r")
        write_file = connection.makefile("w")
        while True:
            line = read_file.readline()
            if len(line) == 0 or line[-1] != "\n":
                break
            print(line.rstrip("\n"))
            time.sleep(0.3)
            write_file.write("mmk\n")
            write_file.flush()


try:
    worker.run_workers("", 48484, LineWorker())
except KeyboardInterrupt:
    print("cancelled")

