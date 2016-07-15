import time
import random

import highfive


def handle_call(call):
    a = call[0]
    b = call[1]
    print("calculation {} + {} ...".format(a, b))
    time.sleep(random.random())
    c = a + b
    print("result: {}".format(c))
    return c


highfive.run_workers(handle_call)

