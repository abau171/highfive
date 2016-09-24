import time
import random

import highfive


# This is the remote worker for the sum example. Here, we define what the
# workers do when they get a call from the master. All we need is a single
# function which takes the call, does some processing, and returns a response.

# An interesting way to play with the workers is to spin some up, then shut
# them down before the job set running on the master is complete. The jobs
# which the workers are running will be requeued on the master so that when
# more workers connect, the jobs will be tried again. This makes network
# problems no big deal as long as you reconnect the workers at some point.


# In our case, we take in a pair of numbers and return their sum. To make
# it easier to watch the progress of the job set in real time, we sleep for
# anywhere between 0 and 1/4 seconds before the sum to simulate heavy
# processing.
def delayed_sum(numbers):

    time.sleep(random.random() / 4)
    return sum(numbers)

# Now we can easily start a worker pool to connect to a local HighFive master.
# We can also add a `host=<host name>` and `port=<port number>` to connect to a
# remote HighFive master. By default, `run_worker_pool()` creates a worker
# process for each available CPU core to maximize CPU utilization, but we can
# we can limit this with `max_workers=<number of workers>`.
if __name__ == "__main__":

    highfive.run_worker_pool(delayed_sum)

