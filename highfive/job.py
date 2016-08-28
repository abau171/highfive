import collections


class Job:

    def get_call(self):
        pass

    def get_result(self, response):
        pass


class JobSet:

    def __init__(self, jobs):
        self._jobs = iter(jobs)
        self._on_deck = None
        self._return_queue = collections.deque()
        self._results = []
        self._active_jobs = 0

        self._load_job()

    def _load_job(self):
        try:
            self._on_deck = next(self._jobs)
            self._active_jobs += 1
        except StopIteration:
            self._on_deck = None

    def jobs_available(self):
        return len(self._return_queue) > 0 or self._on_deck is not None

    def is_complete(self):
        return self._active_jobs == 0

    def get_job(self):
        if len(self._return_queue) > 0:
            return self._return_queue.popleft()
        else:
            job = self._on_deck
            self._load_job()
            return job

    def return_job(self, job):
        self._return_queue.append(job)

    def add_result(self, result):
        self._results.append(result)
        self._active_jobs -= 1


class JobManager:

    def __init__(self, *, loop):
        self._loop = loop
        self._ready = collections.deque()
        self._running = dict() # worker -> running task
        self._active_js = None
        self._js_queue = collections.deque()

    def _distribute_jobs(self):
        if self._active_js is None:
            return
        while self._active_js.is_complete():
            try:
                self._active_js = self._js_queue.popleft()
            except IndexError:
                self._active_js = None
                return
        while self._active_js.jobs_available() and len(self._ready) > 0:
            job = self._active_js.get_job()
            worker = self._ready.popleft()
            task = self._loop.create_task(self._handle_job(job, self._active_js, worker))
            self._running[worker] = task

    async def _handle_job(self, job, js, worker):
        call = job.get_call()
        try:
            response = await worker.make_call(call)
        except:
            js.return_job(job)
            del self._running[worker]
        else:
            result = job.get_result(response)
            js.add_result(result)

            del self._running[worker]
            self._ready.append(worker)
            self._distribute_jobs()

    def add_job_set(self, jobs):
        js = JobSet(jobs)
        if self._active_js is None:
            self._active_js = js
            self._distribute_jobs()
        else:
            self._js_queue.append(js)

    def add_worker(self, worker):
        self._ready.append(worker)
        self._distribute_jobs()

