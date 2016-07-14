import collections
import asyncio
import random

from . import task_set


class MasterServer:

    def __init__(self, loop):
        self._loop = loop
        self._server = None
        self._closing = False
        self._clients = set()
        self._waiters = collections.deque()
        self._task_set_queue = task_set.TaskSetProcessQueue()

    async def _handle(self, reader, writer):
        try:
            while not self._closing:
                ts, task = await self._task_set_queue.next_task()
                await asyncio.sleep(0.15)
                if random.randint(0, 1) == 0:
                    ts.return_task(task)
                    print("RETURNED", task)
                else:
                    ts.task_done()
                    print("DONE", task)
        except task_set.TaskSetQueueClosed:
            pass
        finally:
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
        self._task_set_queue.close()
        self._closing = True
        self._server.close()
        await self._server.wait_closed()
        if len(self._clients) != 0:
            await asyncio.wait(self._clients)
        while len(self._waiters) > 0:
            self._waiters.popleft().set_result(None)

    async def wait_stopped(self):
        waiter = asyncio.Future(loop=self._loop)
        self._waiters.append(waiter)
        await waiter

    async def run_task_set(self, tasks):
        # TODO add wrapper for user
        return self._task_set_queue.run_task_set(tasks)


async def start_master_server(hostname, port, loop):
    server = MasterServer(loop)
    await server.start(hostname, port)
    return server

