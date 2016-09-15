import time
import random

import highfive


def slow_sum(a):
    time.sleep(random.random())
    return sum(a)


highfive.run_worker_pool(slow_sum, max_workers=10)

