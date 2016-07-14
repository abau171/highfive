import asyncio


class MasterServer:

    def __init__(self, loop):
        self._loop = loop
        self._server = None
        self._closing = False
        self._clients = set()

    async def _handle(self, reader, writer):
        while not self._closing:
            await asyncio.sleep(1)
        writer.close()

    async def _accept(self, reader, writer):
        task = self._loop.create_task(self._handle(reader, writer))
        def client_done(task):
            self._clients.remove(task)
        task.add_done_callback(client_done)
        self._clients.add(task)

    async def start(self, hostname, port):
        self._server = await asyncio.streams.start_server(
            self._accept, hostname, port, loop=self._loop)

    async def stop(self):
        self._closing = True
        self._server.close()
        await self._server.wait_closed()
        if len(self._clients) != 0:
            await asyncio.wait(self._clients)

    async def wait_stopped(self):
        await self._server.wait_closed()

    async def run_task_set(self, task_set):
        print("RUN TASK SET:", task_set)


async def start_master_server(hostname, port, loop):
    server = MasterServer(loop)
    await server.start(hostname, port)
    return server

