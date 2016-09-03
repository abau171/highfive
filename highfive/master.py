import random
import asyncio

import job


class FakeWorker:

    def __init__(self, i, *, loop):
        self._loop = loop
        self._i = i
        self._closed = False

    async def make_call(self, call):
        if self._closed:
            raise Exception("fake worker is closed")
        print("CALL TO {}:".format(self._i), call)
        await asyncio.sleep(random.random(), loop=self._loop)
        return call

    def close(self):
        self._closed = True


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
        return self._job_manager.add_job_set(jobs)

    def close(self):
        self._job_manager.close()

    async def wait_closed(self):
        await self._job_manager.wait_closed()


async def main(loop):
    m = Master(loop=loop)
    m.add_worker(-1)
    h = m.run_job_set(FakeJob(i) for i in range(10))
    try:
        while True:
            m.add_worker(await h.next_result())
    except job.EndOfResults:
        pass
    h = m.run_job_set(FakeJob(i) for i in range(10, 20))
    try:
        while True:
            r = await h.next_result()
            print(r)
            if r == 15:
                m.close()
    except job.EndOfResults:
        pass
    await m.wait_closed()

loop = asyncio.get_event_loop()
asyncio.set_event_loop(None)
loop.run_until_complete(main(loop))
loop.close()

