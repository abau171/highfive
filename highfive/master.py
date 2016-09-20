import json
import asyncio

from . import jobs


async def start_master(host="", port=48484, *, loop=None):

    loop = loop if loop is not None else asyncio.get_event_loop()

    server = await loop.create_server(WorkerProtocol, host, port)
    return Master(server, loop=loop)


class WorkerProtocol(asyncio.Protocol):

    def connection_made(self, transport):

        self._transport = transport
        self._buffer = bytearray()
        self._worker = Worker()

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

        self._worker.close()


class Worker:

    def __init__(self):

        self._load_next_job()

    def _load_next_job(self):

        # TODO LOAD JOB PROPERLY
        self._job = jobs.DefaultJob(None)

    def response_received(self, response):

        assert self._job is not None

        result = self._job.get_result(response)
        # TODO REPORT RESULT

        self._load_next_job()

    def close(self):

        if self._job is not None:

            # TODO RETURN JOB
            pass


class Master:

    def __init__(self, server, *, loop):

        self._server = server
        self._loop = loop

    def close(self):

        self._server.close()

    async def wait_closed(self):

        await self._server.wait_closed()

