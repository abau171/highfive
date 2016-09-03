import collections


class Closed(Exception):
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

        return self._active_jobs == 0

    def get_job(self):
        """
        Gets a job from the job set if one is available to be run. The
        jobs_available() method should be consulted first to determine if a
        job can be obtained with this method.
        """

        if self._cancelled:
            raise Closed
        if len(self._return_queue) > 0:
            return self._return_queue.popleft()
        else:
            job = self._on_deck
            self._load_job()
            return job

    def return_job(self, job):
        """
        Returns an incomplete job to the job set to be run again later. If the
        job set has already been cancelled, the job is discarded and the
        active job count is decremented. If no active jobs remain, the job set
        becomes completed.
        """

        if self._cancelled:
            self._active_jobs -= 1
            if self._active_jobs == 0:
                self._results.complete()
        else:
            self._return_queue.append(job)

    def add_result(self, result):
        """
        Adds a result, and decrements the active job count. If no active jobs
        remain, the job set becomes completed. Trying to add a result when the
        job set has been closed will result in a Closed exception being raised.
        """

        if self._cancelled:
            raise Closed
        self._results.add(result)
        self._active_jobs -= 1
        if self._active_jobs == 0:
            self._results.complete()

    def cancel(self):
        """
        Cancels the job set. No more jobs will be returned from get_job(), and
        no more results will be accepted. The job set will still not be
        completed until all jobs are accounted for, and the active job count is
        reduced to 0.
        """

        self._cancelled = True

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

