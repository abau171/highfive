import collections


class ClosedException(Exception):
    """
    Raised when an action cannot be performed because an object has been
    closed.
    """

    pass


class EndOfResults(Exception):
    """
    Raised when a result set has been completed, but the next result is being
    fetched.
    """

    pass


class Job:
    """
    Abstract class for distributable jobs.
    """

    def get_call(self):
        """
        Gets a JSON-serializable call object to send to a worker.
        """

        pass

    def get_result(self, response):
        """
        Gets the result of the job given the response to the job's call from
        a worker.
        """

        pass


class ResultSet:
    """
    A set of job results from a single job set.
    """

    def __init__(self, *, loop):
        self._loop = loop
        self._results = []
        self._complete = False
        self._waiters = []

    def __len__(self):
        """
        The number of results currently available.
        """

        return len(self._results)

    def __getitem__(self, i):
        """
        Gets the ith result if it exists.
        """

        return self._results[i]

    def _change(self):
        """
        Called when a state change has occurred. Waiters are notified that a
        change has occurred.
        """

        for waiter in self._waiters:
            if not waiter.done():
                waiter.set_result(None)
        self._waiters = []

    def add(self, result):
        """
        Adds a new result.
        """

        self._results.append(result)
        self._change()

    def complete(self):
        """
        Indicates that the result set is complete and no new results will be
        added to it in the future.
        """

        self._complete = True
        self._change()

    def is_complete(self):
        """
        Returns whether the result set has been completed.
        """

        return self._complete

    def wait_changed(self):
        """
        Waits until the result set changes. Possible changes can be a result
        being added or the result set being completed. If the result set is
        already completed, this method returns immediately.
        """

        waiter = self._loop.create_future()
        if self.is_complete():
            waiter.set_result(None)
        else:
            self._waiters.append(waiter)
        return waiter


class JobSet:
    """
    A set of jobs to be distributed across the workers. The job set contains
    the state of the execution of the job set, but is controlled by a job
    manager.
    """

    def __init__(self, jobs, *, loop):
        self._loop = loop
        self._jobs = iter(jobs)
        self._on_deck = None
        self._return_queue = collections.deque()
        self._results = ResultSet(loop=self._loop)
        self._active_jobs = 0
        self._cancelled = False

        self._load_job()

    def _load_job(self):
        """
        Loads a job from the job iterator and increments the active job count
        if a job is available.
        """

        try:
            self._on_deck = next(self._jobs)
            self._active_jobs += 1
        except StopIteration:
            self._on_deck = None

    def jobs_available(self):
        """
        Indicates whether the next call to get_job will return a job, or no
        jobs are available to be run.
        """

        if self._cancelled:
            return False
        return len(self._return_queue) > 0 or self._on_deck is not None

    def is_complete(self):
        """
        Indicates whether the job set has been completed.
        """

        if self._cancelled:
            return False
        return self._active_jobs == 0

    def get_job(self):
        """
        Gets a job from the job set if one is available to be run.
        """

        if self._cancelled:
            raise ClosedException
        if len(self._return_queue) > 0:
            return self._return_queue.popleft()
        else:
            job = self._on_deck
            self._load_job()
            return job

    def return_job(self, job):
        """
        Returns an incomplete job to the job set to be run again later.
        """

        self._return_queue.append(job)

    def add_result(self, result):
        """
        Adds a result, and decrements the active job count. If no active jobs
        remain, the job set becomes completed.
        """

        if self._cancelled:
            raise ClosedException
        self._results.add(result)
        self._active_jobs -= 1
        if self._active_jobs == 0:
            self._results.complete()

    def cancel(self):
        """
        Cancels the job set. No more jobs will be returned from get_job(), and
        no more results will be accepted.
        """

        if not self._cancelled:
            self._cancelled = True
            if not self._results.is_complete():
                self._results.complete()

    def get_handle(self):
        """
        Returns a handle for external control of the job set.
        """

        return JobSetHandle(self, self._results)


class JobSetHandle:
    """
    A handle used to externally control job sets without interfering with the
    job manager.
    """

    def __init__(self, js, results):
        self._js = js
        self._results = results
        self._i = 0

    async def next_result(self):
        """
        Gets the next result of the job set, or raises an EndOfResults
        exception if no more results will be found. This method may wait for
        a result to be added if one is not currently available.
        """

        while self._i >= len(self._results):
            if self._results.is_complete():
                raise EndOfResults
            await self._results.wait_changed()
        result = self._results[self._i]
        self._i += 1
        return result


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
        while self._active_js.is_complete():
            try:
                self._active_js = self._js_queue.popleft()
            except IndexError:
                self._active_js = None
                return
        self._assign_ready()

    def _job_fail(self, worker, job):
        """
        Runs when a job has failed for some reason. This indicates that the
        worker is no longer usable and should be closed. The job is returned
        to the job set to be run later, and the worker is closed.
        """

        del self._running[worker]
        self._active_js.return_job(job)
        worker.close()
        if self._closing and len(self._running) == 0:
            self._wakeup()

    def add_job_set(self, jobs):
        """
        Adds a job set to the queue. It is activated immediately if there is
        no active job set. Returns a handle for externally controlling the
        job set.
        """

        if self._closing:
            raise ClosedException
        js = JobSet(jobs, loop=self._loop)
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
            raise ClosedException
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

