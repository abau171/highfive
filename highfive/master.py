import random
import asyncio

import job


class FakeWorker:

    def __init__(self, i, *, loop):
        self._loop = loop
        self._i = i

    async def make_call(self, call):
        print("CALL TO {}:".format(self._i), call)
        await asyncio.sleep(random.random(), loop=self._loop)
        return call


class FakeJob(job.Job):

    def __init__(self, i):
        self._i = i

    def get_call(self):
        return self._i

    def get_result(self, response):
        return response


class Master:

    def __init__(self, *, loop):
        self._loop = loop
        self._job_manager = job.JobManager(loop=self._loop)

    def add_worker(self, i):
        w = FakeWorker(i, loop=self._loop)
        self._job_manager.add_worker(w)

    def run_job_set(self, jobs):
        self._job_manager.add_job_set(jobs)


loop = asyncio.get_event_loop()
asyncio.set_event_loop(None)

m = Master(loop=loop)

m.add_worker(1)
m.add_worker(2)
m.add_worker(3)
m.run_job_set(FakeJob(i) for i in range(10))
m.run_job_set(FakeJob(i) for i in range(10, 20))

try:
    loop.run_forever()
except KeyboardInterrupt:
    loop.close()

