import json

import highfive

class AddTask(highfive.Task):

    def __init__(self, a, b):
        super().__init__()
        self.a = a
        self.b = b

    def run(self, connection):
        connection.send(json.dumps({"a": self.a, "b": self.b}))
        added = json.loads(connection.recv())
        self.done((self.a, self.b, added))

if __name__ == "__main__":

    m = highfive.Master("", 48484)

    p = m.process(AddTask(i, i + 1) for i in range(100))
    for a, b, added in p.results():
        print("{} + {} = {}".format(a, b, added))

