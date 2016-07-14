import time

import highfive


def handle_call(call):
    a = call[0]
    b = call[1]
    print("calculation {} + {} ...".format(a, b))
    time.sleep(0.2)
    c = a + b
    print("result: {}".format(c))
    return c


highfive.run_worker(handle_call)

