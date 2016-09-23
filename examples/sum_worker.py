import time
import random

import highfive


def delayed_sum(iterable):

    time.sleep(random.random() / 4)
    return sum(iterable)

if __name__ == "__main__":

    highfive.run_worker_pool(delayed_sum)

