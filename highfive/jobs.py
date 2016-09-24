import collections
import logging


logger = logging.getLogger(__name__)


class Job:
    """
    Interface for remote jobs.
    """

    def get_call(self):
        """
        Gets a JSON-serializable call object to send to a worker.
        """

        raise NotImplementedError

    def get_result(self, response):
        """
        Gets the result of the job, given the response to the job's call from
        a worker.
        """

        raise NotImplementedError


class DefaultJob(Job):
    """
    Default job which simply provides a preset call object, and returns the raw
    response as the job result.
    """

    def __init__(self, call):

        self._call = call

    def get_call(self):

        return self._call

    def get_result(self, response):

        return response


class Results:
    """
    A set of job results from a single job set.
    """

    def __init__(self, *, loop):
        self._loop = loop
        self._results = []
        self._complete = False
        self._waiters = []

    def __len__(self):

        return len(self._results)

    def __getitem__(self, i):

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

    def aiter(self): # not __aiter__ because we don't want an async function
        """
        Returns an async iterator over the results.
        """

        return ResultsIterator(self)

    def add(self, result):
        """
        Adds a new result.
        """

        assert not self._complete

        self._results.append(result)
        self._change()

    def complete(self):
        """
        Indicates that the result set is complete and no new results will be
        added to it in the future.
        """

        if self._complete:
            return

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
        being added or the result set becoming complete. If the result set is
        already completed, this method returns immediately.
        """

        if not self.is_complete():
            waiter = self._loop.create_future()
            self._waiters.append(waiter)
            await waiter


class ResultsIterator:
    """
    Asynchronous iterator over a Results object. Results are found in the
    order they are added to the results. Getting the next result may block if
    the next result is not known.
    """

    def __init__(self, results):

        self._results = results
        self._i = 0

    async def __aiter__(self):

        return self

    async def __anext__(self):

        if self._i >= len(self._results):

            # We are at the end of the known results. If the results list is
            # complete, this is the permanent end. Otherwise, we have to wait
            # for a change. After the change, if the results list is complete,
            # this is the permanent end, otherwise a new result is available.

            if self._results.is_complete():
                raise StopAsyncIteration
            else:
                await self._results.wait_changed()
                if self._i >= len(self._results):
                    # no new results, change must be results completion
                    raise StopAsyncIteration

        # At this point, the ith result is available

        result = self._results[self._i]
        self._i += 1
        return result


class JobSetHandle:
    """
    A user-friendly object tied to a single job set. It is used to easily
    access job results or cancel the job set.
    """

    def __init__(self, js, results):

        self._js = js
        self._results = results

        self._internal_results_iter = self._results.aiter()

    async def __aenter__(self):

        return self

    async def __aexit__(self, exc_type, exc, tb):

        self.cancel()

    def cancel(self):
        """
        Cancels the job set.
        """

        self._js.cancel()

    def results(self):
        """
        Returns an asynchronous iterator over the all of the job set's results.
        """

        return self._results.aiter()

    async def next_result(self):
        """
        Gets the next result in the job set. An internal result iterator is
        used so multiple calls to next_result() will return subsequent results.
        Calling results() after one or more calls to next_result() will still
        return an iterator over all results, not all remaining results.
        """

        return await self._internal_results_iter.__anext__()


class JobSet:
    """
    A set of jobs to be distributed across the workers. The job set contains
    the state of the execution of the job set, but is controlled by a job
    manager.
    """

    def __init__(self, jobs, results, manager, *, loop):
        self._loop = loop
        self._jobs = iter(jobs)
        self._return_queue = collections.deque()
        self._active_jobs = 0
        self._results = results
        self._manager = manager

        self._waiters = []

        self._load_job()

        if self._active_jobs == 0:
            self._done()

    def _load_job(self):
        """
        If there is still a job in the job iterator, loads it and increments
        the active job count.
        """

        try:
            next_job = next(self._jobs)
        except StopIteration:
            self._on_deck = None
        else:
            if not isinstance(next_job, Job):
                next_job = DefaultJob(next_job)
            self._on_deck = next_job
            self._active_jobs += 1

    def _done(self):
        """
        Marks the job set as completed, and notifies all waiting tasks.
        """

        self._results.complete()
        waiters = self._waiters
        for waiter in waiters:
            waiter.set_result(None)
        self._manager.job_set_done(self)

    def job_available(self):
        """
        Returns True if there is a job queued which can be retrieved by a call
        to get_job(), and False otherwise.
        """

        return len(self._return_queue) > 0 or self._on_deck is not None

    def is_done(self):
        """
        Returns True if the job set is complete, and False otherwise.
        """

        return self._active_jobs == 0

    def get_job(self):
        """
        Gets a job from the job set if one is queued. The jobs_available()
        method should be consulted first to determine if a job can be obtained
        from a call to this method. If no jobs are available, an IndexError is
        raised.
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
        Requeues an incomplete job to be run again later. If the job set is
        already complete, the job is simply discarded instead.
        """

        if self._active_jobs == 0:
            return

        self._return_queue.append(job)

    def add_result(self, result):
        """
        Adds the result of a completed job to the result list, then decrements
        the active job count. If the job set is already complete, the result is
        simply discarded instead.
        """

        if self._active_jobs == 0:
            return

        self._results.add(result)
        self._active_jobs -= 1
        if self._active_jobs == 0:
            self._done()

    def cancel(self):
        """
        Cancels the job set. The job set is immediately finished, and all
        queued jobs are discarded.
        """

        if self._active_jobs == 0:
            return

        self._jobs = iter(())
        self._on_deck = None
        self._return_queue.clear()
        self._active_jobs = 0

        self._done()

    async def wait_done(self):
        """
        Waits until the job set is finished. Returns immediately if the job set
        is already finished.
        """
        
        if self._active_jobs > 0:
            future = self._loop.create_future()
            self._waiters.append(future)
            await future


class JobManager:

    def __init__(self, *, loop):

        self._loop = loop
        self._active_js = None
        self._job_sources = dict()
        self._ready_callbacks = collections.deque()
        self._js_queue = collections.deque()
        self._closed = False

    def _distribute_jobs(self):
        """
        Distributes jobs from the active job set to any waiting get_job
        callbacks.
        """

        while (self._active_js.job_available()
                and len(self._ready_callbacks) > 0):
            job = self._active_js.get_job()
            self._job_sources[job] = self._active_js
            callback = self._ready_callbacks.popleft()
            callback(job)

    def add_job_set(self, job_list):
        """
        Adds a job set to the manager's queue. If there is no job set running,
        it is activated immediately. A new job set handle is returned.
        """

        assert not self._closed

        results = Results(loop=self._loop)
        js = JobSet(job_list, results, self, loop=self._loop)
        if not js.is_done():
            if self._active_js is None:
                self._active_js = js
                logger.debug("activated job set")
                self._distribute_jobs()
            else:
                self._js_queue.append(js)
        else:
            logger.debug("new job set has no jobs")
        return JobSetHandle(js, results)

    def get_job(self, callback):
        """
        Calls the given callback function when a job becomes available.
        """

        assert not self._closed

        if self._active_js is None or not self._active_js.job_available():
            self._ready_callbacks.append(callback)
        else:
            job = self._active_js.get_job()
            self._job_sources[job] = self._active_js
            callback(job)

    def return_job(self, job):
        """
        Returns a job to its source job set to be run again later.
        """

        if self._closed:
            return

        js = self._job_sources[job]
        if len(self._ready_callbacks) > 0:
            callback = self._ready_callbacks.popleft()
            callback(job)
        else:
            del self._job_sources[job]
            js.return_job(job)

    def add_result(self, job, result):
        """
        Adds the result of a job to the results list of the job's source job
        set.
        """

        if self._closed:
            return

        js = self._job_sources[job]
        del self._job_sources[job]
        js.add_result(result)

    def job_set_done(self, js):
        """
        Called when a job set has been completed or cancelled. If the job set
        was active, the next incomplete job set is loaded from the job set
        queue and is activated.
        """

        if self._closed:
            return

        if self._active_js != js:
            return

        try:
            while self._active_js.is_done():
                logger.debug("job set done")
                self._active_js = self._js_queue.popleft()
                logger.debug("activated job set")
        except IndexError:
            self._active_js = None
        else:
            self._distribute_jobs()

    def is_closed(self):
        """
        Returns True if the job manager is closed, and False otherwise.
        """

        return self._closed

    def close(self):
        """
        Closes the job manager. No more jobs will be assigned, no more job sets
        will be added, and any queued or active job sets will be cancelled.
        """

        if self._closed:
            return

        self._closed = True
        if self._active_js is not None:
            self._active_js.cancel()
        for js in self._js_queue:
            js.cancel()

