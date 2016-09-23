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


if __name__ == "__main__":
    unittest.main()

