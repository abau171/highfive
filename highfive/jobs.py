import asyncio
import collections


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

    async def __aiter__(self):

        return ResultsIterator(self)

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

    def __init__(self, results):

        self._results = results
        self._i = 0

    async def __aiter__(self):

        return self

    async def __anext__(self):

        if self._i >= len(self._results):
            if self._results.is_complete():
                raise StopAsyncIteration
            else:
                await self._results.wait_changed()

        if self._i >= len(self._results):
            raise StopAsyncIteration
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

    def __init__(self, jobs, results, *, loop):
        self._loop = loop
        self._jobs = iter(jobs)
        self._return_queue = collections.deque()
        self._active_jobs = 0
        self._results = results

        self._waiters = []
        self._done_callback = None

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
        if self._done_callback is not None:
            self._done_callback()

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

    def set_done_callback(self, callback):

        self._done_callback = callback

class JobManager:

    def __init__(self, *, loop):

        self._loop = loop
        self._active_js = None
        self._job_sources = dict()
        self._ready_callbacks = collections.deque()
        self._js_queue = collections.deque()
        self._closed = False

    def add_job_set(self, job_list):

        assert not self._closed

        results = Results(loop=self._loop)
        js = JobSet(job_list, results, loop=self._loop)
        if self._active_js is None:
            self._active_js = js
            self._active_js.set_done_callback(self.active_job_set_done)
        else:
            self._js_queue.append(js)
        return js, results

    def job_available(self):

        if self._closed or self._active_js is None:
            return False
        else:
            return self._active_js.job_available()

    def get_job(self, callback):

        assert not self._closed

        if self._active_js is None or not self._active_js.job_available():
            self._ready_callbacks.append(callback)
        else:
            job = self._active_js.get_job()
            self._job_sources[job] = self._active_js
            callback(job)

    def return_job(self, job):

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

        if self._closed:
            return

        js = self._job_sources[job]
        del self._job_sources[job]
        js.add_result(result)

    def active_job_set_done(self):

        assert self._active_js.is_done()

        if self._closed:
            return

        try:
            self._active_js = self._js_queue.popleft()
        except IndexError:
            self._active_js = None
        else:
            self._active_js.set_done_callback(self.active_job_set_done)
            while (self._active_js.job_available()
                    and len(self._ready_callbacks) > 0):
                job = self._active_js.get_job()
                self._job_sources[job] = self._active_js
                callback = self._ready_callbacks.popleft()
                callback(job)

    def close(self):

        if self._closed:
            return

        self._closed = True
        if self._active_js is not None:
            self._active_js.cancel()
        for js in self._js_queue:
            js.cancel()

