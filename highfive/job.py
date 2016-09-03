import collections


class ClosedException(Exception):
    pass


class Job:

    def get_call(self):
        pass

    def get_result(self, response):
        pass


class ResultSet:

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
        for waiter in self._waiters:
            if not waiter.done():
                waiter.set_result(None)
        self._waiters = []

    def add(self, result):
        self._results.append(result)
        self._change()

    def complete(self):
        self._complete = True
        self._change()

    def is_complete(self):
        return self._complete

    def wait_changed(self):
        waiter = self._loop.create_future()
        if self.is_complete():
            waiter.set_result(None)
        else:
            self._waiters.append(waiter)
        return waiter


class EndOfResults(Exception):
    pass


class JobSet:

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
        try:
            self._on_deck = next(self._jobs)
            self._active_jobs += 1
        except StopIteration:
            self._on_deck = None

    def jobs_available(self):
        if self._cancelled:
            return False
        return len(self._return_queue) > 0 or self._on_deck is not None

    def is_complete(self):
        if self._cancelled:
            return False
        return self._active_jobs == 0

    def get_job(self):
        if self._cancelled:
            raise ClosedException
        if len(self._return_queue) > 0:
            return self._return_queue.popleft()
        else:
            job = self._on_deck
            self._load_job()
            return job

    def return_job(self, job):
        self._return_queue.append(job)

    def add_result(self, result):
        if self._cancelled:
            raise ClosedException
        self._results.add(result)
        self._active_jobs -= 1
        if self._active_jobs == 0:
            self._results.complete()

    def cancel(self):
        if not self._cancelled:
            self._cancelled = True
            if not self._results.is_complete():
                self._results.complete()

    def get_handle(self):
        return JobSetHandle(self, self._results)


class JobSetHandle:

    def __init__(self, js, results):
        self._js = js
        self._results = results
        self._i = 0

    async def next_result(self):
        while self._i >= len(self._results):
            if self._results.is_complete():
                raise EndOfResults
            await self._results.wait_changed()
        result = self._results[self._i]
        self._i += 1
        return result


class JobManager:

    def __init__(self, *, loop):
        self._loop = loop
        self._ready = collections.deque()
        self._running = dict() # worker -> running task
        self._active_js = None
        self._js_queue = collections.deque()
        self._closing = False
        self._waiters = []

    def _assign(self, worker):
        if self._active_js is not None and self._active_js.jobs_available():
            job = self._active_js.get_job()
            task = self._loop.create_task(self._handle_job(worker, job))
            self._running[worker] = task
        else:
            self._ready.append(worker)

    def _assign_ready(self):
        ready = self._ready
        self._ready = collections.deque()
        for worker in ready:
            self._assign(worker)

    async def _handle_job(self, worker, job):
        call = job.get_call()
        try:
            response = await worker.make_call(call)
        except Exception:
            self._job_fail(worker, job)
        else:
            self._job_complete(worker, job, response)

    def _job_complete(self, worker, job, response):
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
        del self._running[worker]
        self._active_js.return_job(job)
        worker.close()
        if self._closing and len(self._running) == 0:
            self._wakeup()

    def add_job_set(self, jobs):
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
        if self._closing:
            raise ClosedException
        self._assign(worker)

    def close(self):
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
        for waiter in self._waiters:
            if not waiter.done():
                waiter.set_result(None)

    async def wait_closed(self):
        if self._closing and len(self._running) == 0:
            return
        waiter = self._loop.create_future()
        self._waiters.append(waiter)
        await waiter

