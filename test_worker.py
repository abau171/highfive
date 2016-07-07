import time

import highfive


class LineWorker(highfive.Worker):

    def run(self, connection):
        while True:
            print(connection.recv())
            time.sleep(0.3)
            connection.send("mmk")


try:
    highfive.run_workers("", 48484, LineWorker())
except KeyboardInterrupt:
    print("cancelled")

