import socket
import time

import highfive


# This is an example of how to create a HighFive master to run a set of tasks.
# In this example, we are trying to find x where x + x^2 = 29412. We will try
# to accomplish this by trying every x starting at 0, working our way up until
# we find the answer. Obviously, distributed processing is overkill for this,
# but why take all the fun out of it?


# This encapsulates the full state of a single addition between two numbers.
class AddTask(highfive.Task):

    def __init__(self, a, b):
        self._a = a
        self._b = b

    # This prepares a call object which is passed to a single connected
    # worker process. The call object must be JSON-encodable.
    def prepare(self):
        return [self._a, self._b]

    # This receives a result structure which is a JSON-decoded object sent
    # from the worker process that handled this task's call. The result
    # structure is converted into a result object by adding or removing
    # data as needed.
    def finish(self, result_struct):
        return self._a, self._b, result_struct


# To run the tasks, first start up a new HighFive master. The default opens
# a server on port 48484.
with highfive.Master() as m:

    # Now we start a new task set that the master will distribute to the
    # connected workers. All you need to pass in is an iterable object which
    # produces tasks.
    with m.run_task_set(AddTask(x, x ** 2) for x in range(1000)) as ts:

        # We can get results from the tasks as they are returned from the
        # worker processes, and iterate over them like so. Note that results
        # may be returned in a different order than their corresponding tasks.
        for a, b, c in ts.results():
            print("{} + {} = {}".format(a, b, c))

            if c == 29412:
                # Now that we've found the answer, we can simply break out
                # of the for loop which is iterating over results. Once we
                # exit the task set's 'with' block, it will be cancelled
                # so we don't keep running through the rest of the tasks if
                # we've already found the answer we want.
                print("ANSWER: x = {}".format(a))
                break

