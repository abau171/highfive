import logging
import asyncio

import highfive


logging.basicConfig(level=logging.DEBUG)


async def main(loop):

    master = await highfive.start_master(loop=loop)

    js1, results1 = master.run(range(5))
    js2, results2 = master.run(range(0))
    js3, results3 = master.run(range(5))
    js4, results4 = master.run(range(5000))

    async for r in results1:
        print(r)
    async for r in results2:
        print(r)
    async for r in results3:
        print(r)
    async for r in results4:
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

