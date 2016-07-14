import asyncio
import random

from . import task_set


class MasterServer:

    def __init__(self, loop):
        self._loop = loop
        self._server = None
        self._closing = False
        self._clients = set()
        self._waiter = asyncio.Future(loop=self._loop)

        self._cur_task_set = None

    async def _handle(self, reader, writer):
        while not self._closing:
            try:
                task = await self._cur_task_set.next_task()
                await asyncio.sleep(0.15)
                if random.randint(0, 1) == 0:
                    self._cur_task_set.return_task(task)
                    print("RETURNED", task)
                else:
                    print(task)
            except task_set.TaskSetClosed:
                pass
        writer.close()

    async def _accept(self, reader, writer):
        future = asyncio.ensure_future(
            self._handle(reader, writer), loop=self._loop)
        def client_done(future):
            self._clients.remove(future)
        future.add_done_callback(client_done)
        self._clients.add(future)

    async def start(self, hostname, port):
        self._server = await asyncio.streams.start_server(
            self._accept, hostname, port, loop=self._loop)

    async def stop(self):
        self._cur_task_set.cancel()
        self._closing = True
        self._server.close()
        await self._server.wait_closed()
        if len(self._clients) != 0:
            await asyncio.wait(self._clients)
        self._waiter.set_result(None)

    async def wait_stopped(self):
        await self._waiter

    async def run_task_set(self, ts):
        self._cur_task_set = task_set.TaskSetProcess(ts)


async def start_master_server(hostname, port, loop):
    server = MasterServer(loop)
    await server.start(hostname, port)
    return server

