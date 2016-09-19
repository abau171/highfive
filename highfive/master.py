import asyncio
import logging

from . import jobs
from . import manager
from . import server


logger = logging.getLogger(__name__)


class Master:

    def __init__(self, host="", port=48484, *, loop=None):

        self._host = host
        self._port = port
        if loop is None:
            self._loop = asyncio.get_event_loop()
        else:
            self._loop = loop

        self._server = None
        self._manager = None

    async def __aenter__(self):

        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):

        self.close()
        await self.wait_closed()

    async def start(self):

        if self._server is not None:
            raise Exception("master has already been started")

        self._manager = manager.JobManager(loop=self._loop)
        self._server = await server.start_server(
            self._host, self._port, self._manager, loop=self._loop)
        logging.debug("HighFive master started")

    def close(self):

        self._server.close()
        self._manager.close()
        logging.debug("HighFive master closed")

    async def wait_closed(self):

        await self._server.wait_closed()
        await self._manager.wait_closed()

    def run(self, job_iterable):

        js = jobs.JobSet(job_iterable, loop=self._loop)
        self._manager.add_job_set(js)
        return js

