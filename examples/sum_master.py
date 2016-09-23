import asyncio

import highfive


# This example shows how to run a distributed job set with HighFive. The
# scenario is that we want to find x where x + x^2 = 29412, but for some
# reason adding numbers takes a long time. We accomplish this by trying every
# possibility for x up to 1000 and distributing the adds across several
# machines.


# First, we make the job to distribute. This is an indivisible piece of work.
# Its `get_call()` method returns a JSON-serializable call object that is sent
# to a remote worker over a network. We will receive a response from this
# worker, and it will be passed to the `get_result()` method which does any
# extra processing we might need and returns a final result object.
class SumJob(highfive.Job):

    def __init__(self, a, b):

        # We happen to want to add two numbers together, so we'll just take
        # the two numbers as parameters.
        self._a = a
        self._b = b

    def get_call(self):

        # The remote worker needs to know both numbers, so we better send them!
        return [self._a, self._b]

    def get_result(self, response):

        # The response from the remote worker should simply be the sum of the
        # two numbers. We want to print the full mathematical expression, so
        # we'll return a, b, and the sum of them as the result.
        return self._a, self._b, response


async def main(loop):

    # Now we start the HighFive master which internally creates a server to
    # allow workers to connect. We can change the host and port the server is
    # bound to with `host=<host name>` and `port=<port integer>` in the
    # `start_master` method. We get a master object from this method which
    # we'll just call `m`.
    async with await highfive.start_master(loop=loop) as m:

        # We can now start a job set. A job set is just a collection of jobs
        # which are all run together, distributed across the workers. We'll
        # create an iterator that gives us jobs for each x up to 1000. After
        # that, we can start a for loop which waits for each result we find,
        # then prints them. If it finds our target number, it will print out
        # which x value got us that value then break out of the for loop since.
        # we found our answer!
        async with m.run(SumJob(x, x ** 2) for x in range(1000)) as js:
            async for a, b, c in js.results():
                print("{} + {} = {}".format(a, b, c))
                if c == 29412:
                    print("ANSWER: x = {}".format(a))
                    break

        # After we leave the `async with`, the job set is cancelled so we don't
        # waste the remote workers on jobs we don't care about. This would only
        # be important if we had another job set to run. That way the next job
        # set runs immediately after we're done with the last one.

    # We've now left the master's `async with`, which means that the master has
    # been closed and all job sets have been cancelled.


# All we have to do now is set up the event loop and run our `main()` function!
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(None)
    loop.run_until_complete(main(loop))
    loop.close()

