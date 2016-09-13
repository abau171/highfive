import asyncio
import unittest
import random

import highfive.jobs as jobs
import highfive.manager as manager


class MockJob(jobs.Job):

    def __init__(self, i):

        self._i = i

    def get_call(self):

        return self._i

    def get_result(self, response):

        return response


class MockJobSet:

    def __init__(self, n, loop):

        self._n = n
        self._loop = loop

        self._i = 0
        self._num_done = 0
        self._num_returned = 0

        self._waiter = self._loop.create_future()

    def job_available(self):

        return self._i < self._n

    def is_done(self):

        return self._num_done == self._n

    def get_job(self):

        j = MockJob(self._i)
        self._i += 1
        return j

    def return_job(self, job):

        self._num_returned += 1

    def add_result(self, result):

        self._num_done += 1
        if self._num_done == self._n:
            self._waiter.set_result(None)

    async def wait_done(self):

        await self._waiter


class MockWorker:

    def __init__(self, loop):

        self._loop = loop
        self._closed = False

    async def make_call(self, call):

        if self._closed:
            raise Exception("mock worker is closed")

        await asyncio.sleep(random.uniform(0, 0.01), loop=self._loop)
        return call

    def close(self):

        if self._closed:
            raise Exception("mock worker is already closed")

        self._closed = True


class TestJobManager(unittest.TestCase):

    def setUp(self):

        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def test_add_job_set_then_worker(self):

        m = manager.JobManager(loop=self._loop)

        js = MockJobSet(10, self._loop)
        m.add_job_set(js)

        m.add_worker(MockWorker(self._loop))

        self._loop.run_until_complete(js.wait_done())
        self._loop.run_until_complete(asyncio.sleep(0.02, loop=self._loop))
        m.close()
        self._loop.run_until_complete(m.wait_closed())
        self._loop.close()

    def test_add_worker_then_job_set(self):

        m = manager.JobManager(loop=self._loop)

        m.add_worker(MockWorker(self._loop))

        js = MockJobSet(10, self._loop)
        m.add_job_set(js)

        self._loop.run_until_complete(js.wait_done())
        self._loop.run_until_complete(asyncio.sleep(0.1, loop=self._loop))
        m.close()
        self._loop.run_until_complete(m.wait_closed())
        self._loop.close()

    def test_multi_worker(self):

        m = manager.JobManager(loop=self._loop)

        js = MockJobSet(10, self._loop)
        m.add_job_set(js)

        m.add_worker(MockWorker(self._loop))
        m.add_worker(MockWorker(self._loop))
        m.add_worker(MockWorker(self._loop))

        self._loop.run_until_complete(js.wait_done())
        self._loop.run_until_complete(asyncio.sleep(0.02, loop=self._loop))
        m.close()
        self._loop.run_until_complete(m.wait_closed())
        self._loop.close()

    def test_multi_job_set(self):

        m = manager.JobManager(loop=self._loop)

        js1 = MockJobSet(10, self._loop)
        m.add_job_set(js1)

        js2 = MockJobSet(10, self._loop)
        m.add_job_set(js2)

        js3 = MockJobSet(10, self._loop)
        m.add_job_set(js3)

        m.add_worker(MockWorker(self._loop))

        self._loop.run_until_complete(js1.wait_done())
        self._loop.run_until_complete(js2.wait_done())
        self._loop.run_until_complete(js3.wait_done())
        self._loop.run_until_complete(asyncio.sleep(0.02, loop=self._loop))
        m.close()
        self._loop.run_until_complete(m.wait_closed())
        self._loop.close()

    def test_multi_both(self):

        m = manager.JobManager(loop=self._loop)

        js1 = MockJobSet(10, self._loop)
        m.add_job_set(js1)

        js2 = MockJobSet(10, self._loop)
        m.add_job_set(js2)

        js3 = MockJobSet(10, self._loop)
        m.add_job_set(js3)

        m.add_worker(MockWorker(self._loop))
        m.add_worker(MockWorker(self._loop))
        m.add_worker(MockWorker(self._loop))

        self._loop.run_until_complete(js1.wait_done())
        self._loop.run_until_complete(js2.wait_done())
        self._loop.run_until_complete(js3.wait_done())
        self._loop.run_until_complete(asyncio.sleep(0.02, loop=self._loop))
        m.close()
        self._loop.run_until_complete(m.wait_closed())
        self._loop.close()


if __name__ == "__main__":
    unittest.main()

