import asyncio
import collections


class EndOfResults(Exception):
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

        if self.is_complete():
            raise RuntimeError("add result when result set complete")

        self._results.append(result)
        self._change()

    def complete(self):
        """
        Indicates that the result set is complete and no new results will be
        added to it in the future.
        """

        if self.is_complete():
            raise RuntimeError("result set already complete")

        self._complete = True
        self._change()

    def is_complete(self):
        """
        Returns whether the result set has been completed.
        """

        return self._complete

    async def wait_changed(self):
        """
        Waits until the result set changes. Possible changes can be a result
        being added or the result set being completed. If the result set is
        already completed, this method returns immediately.
        """

        if not self.is_complete():
            waiter = self._loop.create_future()
            self._waiters.append(waiter)
            await waiter


class ResultSetIterator:

    def __init__(self, results):

        self._results = results
        self._i = 0

    async def next_result(self):

        if self._i >= len(self._results):
            if self._results.is_complete():
                raise EndOfResults
            else:
                await self._results.wait_changed()

        if self._i >= len(self._results):
            raise EndOfResults
        else:
            result = self._results[self._i]
            self._i += 1
            return result


class JobSet:
    """
    A set of jobs to be distributed across the workers. The job set contains
    the state of the execution of the job set, but is controlled by a job
    manager.
    """

    def __init__(self, jobs, *, loop=None):
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._jobs = iter(jobs)
        self._on_deck = None
        self._return_queue = collections.deque()
        self._results = ResultSet(loop=self._loop)
        self._active_jobs = 0
        self._waiters = []

        self._load_job()

        if self._active_jobs == 0:
            self._done()

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

    def job_available(self):
        """
        Indicates whether the next call to get_job will return a job, or no
        jobs are available to be run.
        """

        return (
            (self._return_queue is not None and len(self._return_queue) > 0)
            or self._on_deck is not None)

    def is_done(self):
        """
        Indicates whether the job set is done executing. This can mean either
        every job has been run successfully or the job set has been cancelled.
        """

        return self._active_jobs == 0

    def get_job(self):
        """
        Gets a job from the job set if one is available to be run. The
        jobs_available() method should be consulted first to determine if a
        job can be obtained with this method.
        """

        if len(self._return_queue) > 0:
            return self._return_queue.popleft()
        elif self._on_deck is not None:
            job = self._on_deck
            self._load_job()
            return job
        else:
            raise IndexError("no jobs available")

    def return_job(self, job):
        """
        Returns an incomplete job to the job set to be run again later. If no
        active jobs remain, the job set becomes completed. If the job set has
        already been cancelled, raises a Closed exception.
        """

        if self.is_done():
            raise RuntimeError("job set is already done, no more jobs may be "
                             + "returned")

        self._return_queue.append(job)

    def add_result(self, result):
        """
        Adds a result, and decrements the active job count. If no active jobs
        remain, the job set becomes completed. Trying to add a result when the
        job set has been closed will result in a Closed exception being raised.
        """

        if self.is_done():
            raise RuntimeError("job set is already done, no more results may "
                             + "be added")

        self._results.add(result)
        self._active_jobs -= 1
        if self._active_jobs == 0:
            self._done()

    def results(self):
        """
        Gets the result set belonging to the job set.
        """

        return self._results

    def cancel(self):
        """
        Cancels the job set. No more jobs will be returned from get_job(), and
        no more results will be accepted.
        """

        if self.is_done():
            raise RuntimeError("job set is already done, it cannot be "
                             + "cancelled")

        self._jobs = None
        self._on_deck = None
        self._return_queue = None
        self._active_jobs = 0

        self._done()

    def _done(self):
        """
        Marks the job set as completed, and notifies all waiting tasks.
        """

        self._results.complete()
        waiters = self._waiters
        self._waiters = None
        for waiter in waiters:
            waiter.set_result(None)

    async def wait_done(self):
        """
        Waits until the job set has completed all jobs or has been cancelled.
        If the job set is already done, returns immediately
        """
        
        if not self.is_done():
            future = self._loop.create_future()
            self._waiters.append(future)
            await future

