import logging
import asyncio

import highfive


logging.basicConfig(level=logging.DEBUG)


async def main(loop):

    master = await highfive.start_master(loop=loop)

    js1 = master.run(range(5))
    js2 = master.run(range(0))
    js3 = master.run(range(5))

    async for r in js1.results():
        print(r)
    async for r in js2.results():
        print(r)
    async for r in js3.results():
        print(r)

    js4 = master.run(range(5000))
    async for r in js4.results():
        print(r)
        if r == 100:
            js4.cancel()
            break # there may be more results, but we want to stop at 100

    master.close()
    await master.wait_closed()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(None)
    loop.run_until_complete(main(loop))
    loop.close()

