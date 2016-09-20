import asyncio

import highfive


loop = asyncio.get_event_loop()
master = loop.run_until_complete(highfive.start_master())

try:
    loop.run_forever()
except KeyboardInterrupt:
    print("keyboard interrupt")

master.close()
loop.run_until_complete(master.wait_closed())
loop.close()

