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

    with m.process(AddTask(i, i + 1) for i in range(20)) as p:
        while True:
            a, b, added = p.next_result()
            print("{} + {} = {}".format(a, b, added))

