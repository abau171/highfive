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

    master = await highfive.start_master(loop=loop)

    js = master.add_job_set(AddJob(i, i * i) for i in range(30))
    async for a, b, c in js.results():
        print("{} + {} = {}".format(a, b, c))

    master.close()
    await master.wait_closed()


if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(None)

    loop.run_until_complete(main(loop))

    loop.close()

