import time
import random

import highfive


# This is an example of how to create a HighFive worker pool. This particular
# example demonstrates addition of two numbers in each call to a worker. This
# file was made to be the worker companion of 'add_master.py', but can be
# used for any worker which requires adding two numbers. Note: please do not
# use HighFive to add two numbers.


# This function is called for each call the HighFive master makes to a
# running worker. It takes two numbers, adds them, then returns the result.
def handle_call(call):

    # The 'call' object is expected to be a list containing two numbers to
    # add to each other. Let's first get those numbers, 'a' and 'b'.
    a = call[0]
    b = call[1]
    print("calculating {} + {} ...".format(a, b))

    # Adding is easy, so lets simulate some calculation time.
    time.sleep(random.random() / 4)
    c = a + b
    print("result: {}".format(c))

    # Finally, return the result to the master.
    return c


# Connect to a local HighFive server, and handle calls using as many
# processes as there are CPU cores available.
highfive.run_workers(handle_call)

