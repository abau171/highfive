import time

import worker


class LineWorker(worker.Worker):

    def run(self, connection):
        while True:
            print(connection.recv())
            time.sleep(0.3)
            connection.send("mmk")


try:
    worker.run_workers("", 48484, LineWorker())
except KeyboardInterrupt:
    print("cancelled")

