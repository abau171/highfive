HighFive
========
Fault-tolerant, cooperative distributed processing framework.

### About

HighFive is a distributed processing framework designed to be used for
collaborative computation. It is made to be fault-tolerant to network issues
and manual disconnects so workers can come and go as they please without
disrupting running processes.

A HighFive master starts a server which allows remote workers to connect and
run tasks from distributed processes managed by the master.

### Installation

HighFive will soon be available on [PyPI](https://pypi.python.org/pypi).

Until then, you can install HighFive by cloning the repository, `cd`-ing into
the root directory, then running `pip install .`.

### Example

The following example is a HighFive instance which attempts to add
numbers multiple times. This is a trivial example, and is intended to
demonstrate the HighFive API rather than a use case.

To run, simply start the Master Script, then start the Worker Script which
will attempt to connect to a local master and execute the tasks.

Master Script (`examples/add_master.py`)
```python
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
```

Worker Script (`examples/add_worker.py`)
```python
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
```
