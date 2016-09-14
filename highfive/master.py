import asyncio

from . import jobs
from . import manager
from . import server


class Master:

    def __init__(self, *, loop):

        self._loop = loop

        self._server = None
        self._manager = None

    async def __aenter__(self):

        return self

    async def __aexit__(self, exc_type, exc, tb):

        self.close()
        await self.wait_closed()

    async def start(self):

        self._manager = manager.JobManager(loop=self._loop)
        self._server = await server.start_server(self._manager, loop=self._loop)

    def close(self):

        self._server.close()
        self._manager.close()

    async def wait_closed(self):

        await self._server.wait_closed()
        await self._manager.wait_closed()

    def add_job_set(self, job_iterable):

        js = jobs.JobSet(job_iterable, loop=self._loop)
        self._manager.add_job_set(js)
        return js


async def start_master(*, loop=None):

    if loop is None:
        loop = asyncio.get_event_loop()

    master = Master(loop=loop)
    await master.start()

    return master

