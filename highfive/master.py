import logging
import json
import asyncio

from . import jobs


logger = logging.getLogger(__name__)


async def start_master(host="", port=48484, *, loop=None):
    """
    Starts a new HighFive master at the given host and port, and returns it.
    """

    loop = loop if loop is not None else asyncio.get_event_loop()

    manager = jobs.JobManager(loop=loop)
    workers = set()
    server = await loop.create_server(
            lambda: WorkerProtocol(manager, workers), host, port)
    return Master(server, manager, workers, loop=loop)


class WorkerProtocol(asyncio.Protocol):
    """
    The asyncio protocol used to handle remote workers. This class finds lines
    of input and delegates their processing to a Worker object.
    """

    def __init__(self, manager, workers):

        self._manager = manager
        self._workers = workers

    def connection_made(self, transport):
        """
        Called when a remote worker connection has been found. Finishes setting
        up the protocol object.
        """

        if self._manager.is_closed():
            logger.debug("worker tried to connect while manager was closed")
            return

        logger.debug("new worker connected")

        self._transport = transport
        self._buffer = bytearray()
        self._worker = Worker(self._transport, self._manager)
        self._workers.add(self._worker)

    def data_received(self, data):
        """
        Called when a chunk of data is received from the remote worker. These
        chunks are stored in a buffer. When a complete line is found in the
        buffer, it removed and sent to line_received().
        """

        self._buffer.extend(data)
        while True:
            i = self._buffer.find(b"\n")
            if i == -1:
                break
            line = self._buffer[:i+1]
            self._buffer = self._buffer[i+1:]
            self.line_received(line)

    def line_received(self, line):
        """
        Called when a complete line is found from the remote worker. Decodes
        a response object from the line, then passes it to the worker object.
        """

        response = json.loads(line.decode("utf-8"))
        self._worker.response_received(response)

    def connection_lost(self, exc):
        """
        Called when the connection to the remote worker is broken. Closes the
        worker.
        """

        logger.debug("worker connection lost")

        self._worker.close()
        self._workers.remove(self._worker)


class Worker:
    """
    Handles job retrieval and result reporting for remote workers.
    """

    def __init__(self, transport, manager):

        self._transport = transport
        self._manager = manager

        self._closed = False

        self._load_job()

    def _load_job(self):
        """
        Initiates a job load from the job manager.
        """

        self._job = None
        self._manager.get_job(self._job_loaded)

    def _job_loaded(self, job):
        """
        Called when a job has been found for the worker to run. Sends the job's
        RPC to the remote worker.
        """

        logger.debug("worker {} found a job".format(id(self)))

        if self._closed:
            self._manager.return_job(job)
            return

        self._job = job
        call_obj = self._job.get_call()
        call = (json.dumps(call_obj) + "\n").encode("utf-8")
        self._transport.write(call)

    def response_received(self, response):
        """
        Called when a response to a job RPC has been received. Decodes the
        response and finalizes the result, then reports the result to the
        job manager.
        """

        if self._closed:
            return

        assert self._job is not None

        logger.debug("worker {} got response".format(id(self)))
        result = self._job.get_result(response)
        self._manager.add_result(self._job, result)

        self._load_job()

    def close(self):
        """
        Closes the worker. No more jobs will be handled by the worker, and any
        running job is immediately returned to the job manager.
        """

        if self._closed:
            return

        self._closed = True

        if self._job is not None:
            self._manager.return_job(self._job)
            self._job = None


class Master:

    def __init__(self, server, manager, workers, *, loop):

        self._server = server
        self._manager = manager
        self._workers = workers
        self._loop = loop

        self._closed = False

    async def __aenter__(self):

        return self

    async def __aexit__(self, exc_type, exc, tb):

        self.close()
        await self.wait_closed()

    def run(self, job_list):
        """
        Runs a job set which consists of the jobs in an iterable job list.
        """

        if self._closed:
            raise RuntimeError("master is closed")

        return self._manager.add_job_set(job_list)

    def close(self):
        """
        Starts closing the HighFive master. The server will be closed and
        all queued job sets will be cancelled.
        """

        if self._closed:
            return

        self._closed = True

        self._server.close()
        self._manager.close()
        for worker in self._workers:
            worker.close()

    async def wait_closed(self):
        """
        Waits until the HighFive master closes completely.
        """

        await self._server.wait_closed()

