import asyncio
import threading
import socket


class MasterServer:

    def __init__(self, loop):
        self._loop = loop
        self._server = None
        self._closing = False
        self._clients = set()
        self._job_queue = asyncio.Queue(loop=self._loop)
        self._result_queue = asyncio.Queue(loop=self._loop)

    async def _handle(self, reader, writer):
        while True:
            job = None
            while not self._closing:
                try:
                    job = await asyncio.wait_for(self._job_queue.get(), 1)
                    break
                except asyncio.TimeoutError:
                    pass
            if self._closing:
                break
            await asyncio.sleep(1)
            self._result_queue.put_nowait(job + 1)
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

    async def add_job(self, job):
        self._job_queue.put_nowait(job)

    async def get_result(self):
        return await self._result_queue.get()


async def start_master_server(hostname, port, loop):
    server = MasterServer(loop)
    await server.start(hostname, port)
    return server


class Master:

    def __init__(self, hostname, port):
        self._hostname = hostname
        self._port = port
        self._loop = None
        self._server = None
        self._thread = None

    def _run_coro(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def start(self):
        self._loop = asyncio.new_event_loop()
        self._server = self._loop.run_until_complete(start_master_server(self._hostname, self._port, self._loop))
        def run_master_thread(server, loop):
            asyncio.set_event_loop(loop)
            loop.run_until_complete(server.wait_stopped())
            loop.close()
        self._thread = threading.Thread(
            target=run_master_thread, args=(self._server, self._loop))
        self._thread.start()

    def stop(self):
        self._run_coro(self._server.stop())

    def add_job(self, job):
        self._run_coro(self._server.add_job(job))

    def get_result(self):
        return self._run_coro(self._server.get_result())


with Master("", 48484) as m:
    for i in range(10):
        m.add_job(i)
    s = socket.socket()
    s.connect(("", 48484))
    for _ in range(5):
        print(m.get_result())
    s.close()

