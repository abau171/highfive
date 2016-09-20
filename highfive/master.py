import logging
import json
import asyncio

from . import jobs


logger = logging.getLogger(__name__)


async def start_master(host="", port=48484, *, loop=None):

    loop = loop if loop is not None else asyncio.get_event_loop()

    js = jobs.JobSet(range(10), loop=loop)
    server = await loop.create_server(
            lambda: WorkerProtocol(js), host, port)
    return Master(server, loop=loop)


class WorkerProtocol(asyncio.Protocol):

    def __init__(self, js):

        self._js = js

    def connection_made(self, transport):

        logger.debug("new worker connected")

        self._transport = transport
        self._buffer = bytearray()
        self._worker = Worker(self._transport, self._js)

    def data_received(self, data):

        self._buffer.extend(data)
        while True:
            i = self._buffer.find(b"\n")
            if i == -1:
                break
            line = self._buffer[:i+1]
            self._buffer = self._buffer[i+1:]
            self.line_received(line)

    def line_received(self, line):

        response = json.loads(line.decode("utf-8"))
        self._worker.response_received(response)

    def connection_lost(self, exc):

        logger.debug("worker connection lost")

        self._worker.close()


class Worker:

    def __init__(self, transport, js):

        self._transport = transport
        self._js = js

        self._load_job()

    def _load_job(self):

        try:
            logger.debug("worker {} found a job".format(id(self)))
            self._job = self._js.get_job()
            call_obj = self._job.get_call()
            call = (json.dumps(call_obj) + "\n").encode("utf-8")
            self._transport.write(call)
        except IndexError:
            logger.debug("worker {} could not find a job".format(id(self)))
            self._job = None

    def response_received(self, response):

        assert self._job is not None

        logger.debug("worker {} got response".format(id(self)))
        result = self._job.get_result(response)
        self._js.add_result(result)

        self._load_job()

    def close(self):

        if self._job is not None:
            self._js.return_job(self._job)


class Master:

    def __init__(self, server, *, loop):

        self._server = server
        self._loop = loop

    def close(self):

        self._server.close()

    async def wait_closed(self):

        await self._server.wait_closed()

