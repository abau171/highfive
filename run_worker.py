import asyncio
import json
import random

async def main():
    reader, writer = await asyncio.open_connection("localhost", 48484)
    try:
        while True:
            a, b = json.loads((await reader.readline()).decode("utf-8"))
            await asyncio.sleep(random.random())
            writer.write((json.dumps(a + b) + "\n").encode("utf-8"))
    except json.JSONDecodeError:
        pass
    writer.close()

asyncio.get_event_loop().run_until_complete(main())

