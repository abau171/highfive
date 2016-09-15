import asyncio

import highfive


class AddJob(highfive.Job):

    def __init__(self, a, b):

        self._a = a
        self._b = b

    def get_call(self):

        return [self._a, self._b]

    def get_result(self, response):

        return (self._a, self._b, response)


async def main(loop):

    async with await highfive.start_master(loop=loop) as master:

        jobs = (AddJob(i, i * i) for i in range(100, 200))
        async with master.add_job_set(jobs) as js:
            async for a, b, c in js.results():
                print("{} + {} = {}".format(a, b, c))
                if a == 150:
                    break

        jobs = ([i, i * i] for i in range(100))
        async with master.add_job_set(jobs) as js:
            async for c in js.results():
                print(c)


if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(None)

    loop.run_until_complete(main(loop))

    loop.close()

