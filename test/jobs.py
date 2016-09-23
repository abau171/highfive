import asyncio
import unittest

import highfive.jobs as jobs


class TestDefaultJob(unittest.TestCase):

    def test_get(self):

        call = object()
        job = jobs.DefaultJob(call)

        got_call = job.get_call()
        self.assertIs(got_call, call)

    def test_result(self):

        job = jobs.DefaultJob(object())

        response = object()
        result = job.get_result(response)
        self.assertIs(result, response)


class TestResults(unittest.TestCase):

    def test_empty(self):

        results = jobs.Results(loop=None)

        self.assertEqual(len(results), 0)
        self.assertFalse(results.is_complete())

    def test_3_results(self):

        results = jobs.Results(loop=None)

        for i in range(3):
            results.add(i)

        self.assertEqual(len(results), 3)
        for i in range(3):
            self.assertEqual(results[i], i)

    def test_complete(self):

        results = jobs.Results(loop=None)

        for i in range(3):
            results.add(i)

        self.assertFalse(results.is_complete())

        results.complete()

        self.assertTrue(results.is_complete())


class MockResults:

    def __init__(self):

        self._results = []
        self._complete = False

    def add(self, result):

        assert not self._complete
        self._results.append(result)

    def complete(self):

        assert not self._complete
        self._complete = True


class MockManager:

    def __init__(self):

        self._done = set()

    def job_set_done(self, js):

        self._done.add(js)


class TestJobSet(unittest.TestCase):

    def test_empty(self):

        r = MockResults()
        m = MockManager()
        js = jobs.JobSet(range(0), r, m, loop=None)

        self.assertFalse(js.job_available())
        self.assertTrue(js.is_done())
        self.assertIn(js, m._done)

    def test_normal(self):

        r = MockResults()
        m = MockManager()
        js = jobs.JobSet(range(2), r, m, loop=None)

        self.assertTrue(js.job_available())
        self.assertFalse(js.is_done())

        j = js.get_job()
        js.add_result(j)

        self.assertTrue(js.job_available())
        self.assertFalse(js.is_done())
        self.assertEqual(len(r._results), 1)
        self.assertFalse(r._complete)

        j = js.get_job()

        self.assertFalse(js.job_available())
        self.assertFalse(js.is_done())

        js.add_result(j)

        self.assertFalse(js.job_available())
        self.assertTrue(js.is_done())
        self.assertEqual(len(r._results), 2)
        self.assertTrue(r._complete)

    def test_return(self):

        r = MockResults()
        m = MockManager()
        js = jobs.JobSet(range(1), r, m, loop=None)

        self.assertTrue(js.job_available())
        self.assertFalse(js.is_done())

        j = js.get_job()

        self.assertFalse(js.job_available())
        self.assertFalse(js.is_done())

        js.return_job(j)

        self.assertTrue(js.job_available())
        self.assertFalse(js.is_done())

        j = js.get_job()

        self.assertFalse(js.job_available())
        self.assertFalse(js.is_done())

        js.add_result(j)

        self.assertFalse(js.job_available())
        self.assertTrue(js.is_done())

    def test_take_2_return_first(self):

        r = MockResults()
        m = MockManager()
        js = jobs.JobSet(range(2), r, m, loop=None)

        self.assertTrue(js.job_available())
        self.assertFalse(js.is_done())

        j1 = js.get_job()
        j2 = js.get_job()

        self.assertFalse(js.job_available())
        self.assertFalse(js.is_done())

        js.return_job(j1)
        js.add_result(j2)

        self.assertTrue(js.job_available())
        self.assertFalse(js.is_done())

        j1 = js.get_job()

        self.assertFalse(js.job_available())
        self.assertFalse(js.is_done())

        js.add_result(j1)

        self.assertFalse(js.job_available())
        self.assertTrue(js.is_done())

    def test_cancel(self):

        r = MockResults()
        m = MockManager()
        js = jobs.JobSet(range(5), r, m, loop=None)

        j1 = js.get_job()
        j2 = js.get_job()
        j3 = js.get_job()
        j4 = js.get_job()

        js.add_result(j1)
        js.return_job(j2)

        self.assertTrue(js.job_available())
        self.assertFalse(js.is_done())

        js.cancel()

        self.assertFalse(js.job_available())
        self.assertTrue(js.is_done())

        js.add_result(j3)
        js.return_job(j4)

        self.assertFalse(js.job_available())
        self.assertTrue(js.is_done())


class JobGetter:

    def __init__(self):
        self._job = None

    def callback(self, job):
        self._job = job


class TestJobManager(unittest.TestCase):

    def test_no_js(self):

        m = jobs.JobManager(loop=None)

        self.assertFalse(m.is_closed())

        g = JobGetter()
        m.get_job(g.callback)

        self.assertIsNone(g._job)

        m.close()

        self.assertTrue(m.is_closed())
        self.assertIsNone(g._job)

    def test_1_js_1_job(self):

        m = jobs.JobManager(loop=None)

        js = m.add_job_set(range(1))

        g = JobGetter()
        m.get_job(g.callback)
        j = g._job
        c = j.get_call()

        self.assertEqual(c, 0)

        m.add_result(j, c)

        g = JobGetter()
        m.get_job(g.callback)

        self.assertIsNone(g._job)

        m.close()

    def test_1_js_2_job(self):

        m = jobs.JobManager(loop=None)

        js = m.add_job_set(range(2))

        g1 = JobGetter()
        m.get_job(g1.callback)
        j1 = g1._job
        c1 = j1.get_call()

        g2 = JobGetter()
        m.get_job(g2.callback)
        j2 = g2._job
        c2 = j2.get_call()

        self.assertEqual(c1, 0)
        self.assertEqual(c2, 1)

        m.add_result(j1, c1)
        m.add_result(j2, c2)

        g = JobGetter()
        m.get_job(g.callback)

        self.assertIsNone(g._job)

        m.close()

    def test_1_js_2_job_return_1(self):

        m = jobs.JobManager(loop=None)

        js = m.add_job_set(range(2))

        g1 = JobGetter()
        m.get_job(g1.callback)
        j1 = g1._job
        c1 = j1.get_call()

        g2 = JobGetter()
        m.get_job(g2.callback)
        j2 = g2._job
        c2 = j2.get_call()

        self.assertEqual(c1, 0)
        self.assertEqual(c2, 1)

        m.add_result(j1, c1)
        m.return_job(j2)

        g = JobGetter()
        m.get_job(g.callback)
        j2 = g2._job
        c2 = j2.get_call()

        self.assertEqual(c2, 1)

        m.add_result(j2, c2)

        g = JobGetter()
        m.get_job(g.callback)
        self.assertIsNone(g._job)

        m.close()

    def test_2_js_1_job(self):

        m = jobs.JobManager(loop=None)

        js1 = m.add_job_set(range(1))
        js2 = m.add_job_set(range(1))

        g1 = JobGetter()
        m.get_job(g1.callback)
        j1 = g1._job
        c1 = j1.get_call()

        g2 = JobGetter()
        m.get_job(g2.callback)
        j2 = g2._job

        self.assertEqual(c1, 0)
        self.assertIsNone(j2)

        m.add_result(j1, c1)

        j2 = g2._job

        self.assertIsNotNone(j2)

        c2 = j2.get_call()
        m.add_result(j2, c2)

        g = JobGetter()
        m.get_job(g.callback)
        self.assertIsNone(g._job)

        m.close()

    def test_2_js_staggered_1_job(self):

        m = jobs.JobManager(loop=None)

        js1 = m.add_job_set(range(1))

        g1 = JobGetter()
        m.get_job(g1.callback)
        j1 = g1._job
        c1 = j1.get_call()

        g2 = JobGetter()
        m.get_job(g2.callback)
        j2 = g2._job

        self.assertEqual(c1, 0)
        self.assertIsNone(j2)

        m.add_result(j1, c1)

        j2 = g2._job

        self.assertIsNone(j2)

        js2 = m.add_job_set(range(1))
        j2 = g2._job

        self.assertIsNotNone(j2)

        c2 = j2.get_call()
        m.add_result(j2, c2)

        g = JobGetter()
        m.get_job(g.callback)
        self.assertIsNone(g._job)

        m.close()


if __name__ == "__main__":
    unittest.main()

