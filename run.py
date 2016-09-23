import logging
import asyncio

import highfive


logging.basicConfig(level=logging.DEBUG)


async def main(loop):

    async with await highfive.start_master(loop=loop) as master:

        js1 = master.run(range(5))
        js2 = master.run(range(0))
        js3 = master.run(range(5))

        async for r in js1.results():
            print(r)
        async for r in js2.results():
            print(r)
        async for r in js3.results():
            print(r)

        async with master.run(range(5000)) as js4:
            async for r in js4.results():
                print(r)
                if r == 100:
                    break # there may be more results, but stop at 100
        js5 = master.run(range(1))
        async for r in js5.results():
            print(r)

        async for r in master.run(range(500000)).results():
            print(r)
            if r == 200:
                break

        # uncomment to run last job set to completion (because master isn't
        # closed until this next one is completed, but it can't be completed
        # until the last one is done.).
        #async for r in master.run(range(1)).results():
        #    pass


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(None)
    loop.run_until_complete(main(loop))
    loop.close()

