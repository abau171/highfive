import collections

import jobset


class Closed(Exception):
    """
    Raised when an action cannot be performed because an object has been
    closed.
    """

    pass


class JobManager:
    """
    Manages the job sets and assignment of jobs to available workers.
    """

    def __init__(self, *, loop):
        self._loop = loop
        self._ready = collections.deque()
        self._running = dict() # worker -> running task
        self._active_js = None
        self._js_queue = collections.deque()
        self._closing = False
        self._waiters = []

    def _assign(self, worker):
        """
        Assigns a job to a worker, or adds the worker to the ready queue if no
        job is currently available.
        """

        if self._active_js is not None and self._active_js.jobs_available():
            job = self._active_js.get_job()
            task = self._loop.create_task(self._handle_job(worker, job))
            self._running[worker] = task
        else:
            self._ready.append(worker)

    def _assign_ready(self):
        """
        Assigns all workers waiting in the ready queue.
        """

        ready = self._ready
        self._ready = collections.deque()
        for worker in ready:
            self._assign(worker)

    def _check_js_complete(self):
        """
        If the current active job set has been completed, the next one from
        the job set queue is activated if one exists and the ready workers
        are run.
        """

        while self._active_js.is_complete():
            try:
                self._active_js = self._js_queue.popleft()
            except IndexError:
                self._active_js = None
                return
        self._assign_ready()

    async def _handle_job(self, worker, job):
        """
        Runs a job on a worker, then handles its success or failure.
        """

        call = job.get_call()
        try:
            response = await worker.make_call(call)
        except Exception:
            self._job_fail(worker, job)
        else:
            self._job_complete(worker, job, response)

    def _job_complete(self, worker, job, response):
        """
        Runs when a job has been completed successfully. Reports the result to
        the active job set, and reassigns the worker. If the active job set
        is completed, the next job set is activated.
        """

        del self._running[worker]
        self._assign(worker)
        result = job.get_result(response)
        self._active_js.add_result(result)
        self._check_js_complete()

    def _job_fail(self, worker, job):
        """
        Runs when a job has failed for some reason. This indicates that the
        worker is no longer usable and should be closed. The job is returned
        to the job set to be run later, and the worker is closed. If the
        active job set is completed, the next job set is activated.
        """

        del self._running[worker]
        worker.close()
        self._active_js.return_job(job)
        self._check_js_complete()
        if self._closing and len(self._running) == 0:
            self._wakeup()

    def add_job_set(self, jobs):
        """
        Adds a job set to the queue. It is activated immediately if there is
        no active job set. Returns a handle for externally controlling the
        job set.
        """

        if self._closing:
            raise Closed
        js = jobset.JobSet(jobs, loop=self._loop)
        if self._active_js is None:
            self._active_js = js
            self._assign_ready()
        else:
            self._js_queue.append(js)
        return js.get_handle()

    def add_worker(self, worker):
        """
        Adds a worker and assigns a job for it to run if one is available.
        """

        if self._closing:
            raise Closed
        self._assign(worker)

    def close(self):
        """
        Starts closing the job manager. All waiting workers are closed
        immediately, all worker tasks are cancelled, all job sets including the
        active job set are cancelled. The manager is not closed immediately,
        and wait_closed() should be called to wait until it is closed.
        """

        if self._closing:
            return
        self._closing = True
        while len(self._ready) > 0:
            worker = self._ready.pop()
            worker.close()
        for task in self._running.values():
            task.cancel()
        while len(self._js_queue) > 0:
            js = self._js_queue.pop()
            js.cancel()
        if self._active_js is not None:
            self._active_js.cancel()
        if len(self._running) == 0:
            self._wakeup()

    def _wakeup(self):
        """
        Called when the manager is finished closing. Sets the result on all
        futures waiting on the close.
        """

        for waiter in self._waiters:
            if not waiter.done():
                waiter.set_result(None)

    async def wait_closed(self):
        """
        Waits for the job manager to close. Returns immediately if the job
        manager is already closed.
        """

        if self._closing and len(self._running) == 0:
            return
        waiter = self._loop.create_future()
        self._waiters.append(waiter)
        await waiter

