import logging
import json
import asyncio

from . import jobs


logger = logging.getLogger(__name__)


async def start_master(host="", port=48484, *, loop=None):

    loop = loop if loop is not None else asyncio.get_event_loop()

    manager = jobs.JobManager(loop=loop)
    server = await loop.create_server(
            lambda: WorkerProtocol(manager), host, port)
    return Master(server, manager, loop=loop)


class WorkerProtocol(asyncio.Protocol):

    def __init__(self, manager):

        self._manager = manager

    def connection_made(self, transport):

        logger.debug("new worker connected")

        self._transport = transport
        self._buffer = bytearray()
        self._worker = Worker(self._transport, self._manager)

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

    def __init__(self, transport, manager):

        self._transport = transport
        self._manager = manager

        self._load_job()

    def _load_job(self):

        try:
            self._job = self._manager.get_job()
        except IndexError:
            logger.debug("worker {} could not find a job".format(id(self)))
            self._job = None
        else:
            logger.debug("worker {} found a job".format(id(self)))
            call_obj = self._job.get_call()
            call = (json.dumps(call_obj) + "\n").encode("utf-8")
            self._transport.write(call)

    def response_received(self, response):

        assert self._job is not None

        logger.debug("worker {} got response".format(id(self)))
        result = self._job.get_result(response)
        self._manager.add_result(self._job, result)

        self._load_job()

    def close(self):

        if self._job is not None:
            self._manager.return_job(self._job)
            self._job = None


class Master:

    def __init__(self, server, manager, *, loop):

        self._server = server
        self._manager = manager
        self._loop = loop

    def run(self, job_list):

        return self._manager.add_job_set(job_list)

    def close(self):

        self._server.close()

    async def wait_closed(self):

        await self._server.wait_closed()

