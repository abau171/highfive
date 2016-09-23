import logging
import asyncio

import highfive


logging.basicConfig(level=logging.DEBUG)


loop = asyncio.get_event_loop()
master = loop.run_until_complete(highfive.start_master())
master.run(range(0))
master.run(range(5))
master.run(range(0))
master.run(range(5))
master.run(range(1))
master.run(range(5))

try:
    loop.run_forever()
except KeyboardInterrupt:
    print("keyboard interrupt")

master.close()
loop.run_until_complete(master.wait_closed())
loop.close()

