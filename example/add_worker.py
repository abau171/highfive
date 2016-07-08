import json

import highfive


class AddWorker(highfive.Worker):

    def run(self, connection):
        while True:
            params = json.loads(connection.recv())
            result = params["a"] + params["b"]
            connection.send(json.dumps(result))

if __name__ == "__main__":

    highfive.run_workers("", 48484, AddWorker())

