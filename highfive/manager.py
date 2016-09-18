import asyncio
import collections
import logging

from . import jobs


logger = logging.getLogger(__name__)


class JobManager:
    """
    Manages the distribution of jobs from multiple job sets to connected
    workers.
    """

    def __init__(self, *, loop):
        self._loop = loop

        self._closing = False

        self._ready = collections.deque()
        self._running = None

        self._active_js = None
        self._active_task = None
        self._js_queue = collections.deque()

        self._waiters = []

    async def _run_job(self, worker, job):
        call = job.get_call()
        try:
            response = await worker.make_call(call)
            result = job.get_result(response)
        except Exception:
            logger.debug("worker could not finish job, closing")
            del self._running[worker]
            worker.close()
            if not self._active_js.is_done():
                self._active_js.return_job(job)
        else:
            logger.debug("worker finished job")
            del self._running[worker]
            if not self._closing:
                self._assign(worker)
            if not self._active_js.is_done():
                self._active_js.add_result(result)

    def _assign(self, worker):
        if self._active_js is not None and self._active_js.job_available():
            logging.debug("worker found job")
            job = self._active_js.get_job()
            self._running[worker] = self._loop.create_task(self._run_job(worker, job))
        else:
            logging.debug("worker found no job, added to ready queue")
            self._ready.append(worker)

    def _activate(self, js):
        logger.debug("running next job set")
        self._active_js = js
        self._active_task = self._loop.create_task(self._run_active_js())
        self._running = dict()
        ready = self._ready
        self._ready = collections.deque()
        for worker in ready:
            self._assign(worker)

    async def _run_active_js(self):
        await self._active_js.wait_done()
        tasks = self._running.values()
        if len(tasks) > 0:
            await asyncio.wait(tasks, loop=self._loop)
        logger.debug("job set complete")
        if not self._closing and len(self._js_queue) > 0:
            next_js = self._js_queue.pop()
            self._activate(next_js)
        else:
            self._active_js = None
            self._active_task = None
            self._running = None
            if self._closing:
                self._close()

    def _close(self):
        waiters = self._waiters
        self._waiters = None
        for waiter in waiters:
            waiter.set_result(None)

    def add_worker(self, worker):
        if self._closing:
            logger.debug("attempted to add worker during close")
            worker.close()
        else:
            logger.debug("worker added")
            self._assign(worker)

    def add_job_set(self, js):
        if self._closing:
            raise Exception("job set can't be added: manager is closing")
        logger.debug("job set added")
        if self._active_js is None:
            self._activate(js)
        else:
            self._js_queue.append(js)

    def close(self):
        if self._closing:
            return
        self._closing = True
        for worker in self._ready:
            worker.close()
        self._ready = None
        if self._active_js is not None and not self._active_js.is_done():
            self._active_js.cancel()
        for js in self._js_queue:
            js.cancel()
        self._js_queue = None

    async def wait_closed(self):
        if not self._closing or self._active_js is not None:
            future = self._loop.create_future()
            self._waiters.append(future)
            await future

