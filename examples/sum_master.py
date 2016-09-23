import asyncio

import highfive


class SumJob(highfive.Job):

    def __init__(self, a, b):

        self._a = a
        self._b = b

    def get_call(self):

        return [self._a, self._b]

    def get_result(self, response):

        return self._a, self._b, response


async def main(loop):

    async with await highfive.start_master(loop=loop) as m:

        async with m.run(SumJob(x, x ** 2) for x in range(1000)) as js:
            async for a, b, c in js.results():
                print("{} + {} = {}".format(a, b, c))
                if c == 29412:
                    print("ANSWER: x = {}".format(a))
                    break

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(None)
    loop.run_until_complete(main(loop))
    loop.close()

