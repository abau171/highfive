import asyncio
import unittest

import highfive.jobs as jobs


class TestJobSet(unittest.TestCase):

    def setUp(self):

        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def test_no_jobs(self):
        """
        Ensure that a job set initialized with an empty job iterable is
        treated as a fully finished job set.
        """

        js = jobs.JobSet(range(0), loop=self._loop)

        self.assertTrue(js.is_done())
        self.assertFalse(js.job_available())
        with self.assertRaises(IndexError):
            js.get_job()
        self.assertTrue(js.results().is_complete())
        self.assertEqual(len(js.results()), 0)

    def test_one_job(self):
        """
        Runs through the normal execution of a job set with a single job. The
        job set is created, a job is fetched, then a result is added. This
        leaves the job set in a finished state.
        """

        js = jobs.JobSet(range(1), loop=self._loop)

        self.assertFalse(js.is_done())
        self.assertTrue(js.job_available())

        job = js.get_job()

        self.assertFalse(js.is_done())
        self.assertFalse(js.job_available())

        js.add_result(None)

        self.assertTrue(js.is_done())
        self.assertFalse(js.job_available())
        self.assertTrue(js.results().is_complete())
        self.assertEqual(len(js.results()), 1)

    def test_one_job_return(self):
        """
        Runs through the normal execution of a job set with a single job,
        except the first job run fails and the job is returned to the queue.
        The job set is created, a job is fetched, then the job is returned to
        the queue. The job is then fetched again, then a result is added. This
        leaves the job set in a finished state.
        """

        js = jobs.JobSet(range(1), loop=self._loop)

        job = js.get_job()
        js.return_job(job)

        self.assertFalse(js.is_done())
        self.assertTrue(js.job_available())

        job = js.get_job()

        self.assertFalse(js.is_done())
        self.assertFalse(js.job_available())

        js.add_result(None)

        self.assertTrue(js.is_done())
        self.assertFalse(js.job_available())
        self.assertTrue(js.results().is_complete())
        self.assertEqual(len(js.results()), 1)

    def test_many_jobs_mixed(self):
        """
        Creates several jobs and runs them through several different means of
        execution concurrently.
        """

        js = jobs.JobSet(range(3), loop=self._loop)

        self.assertFalse(js.is_done())
        self.assertTrue(js.job_available())

        job1 = js.get_job()

        self.assertFalse(js.is_done())
        self.assertTrue(js.job_available())

        job2 = js.get_job()
        job3 = js.get_job()

        self.assertFalse(js.is_done())
        self.assertFalse(js.job_available())

        js.add_result(None)

        self.assertFalse(js.is_done())
        self.assertFalse(js.job_available())

        js.return_job(job2)

        self.assertFalse(js.is_done())
        self.assertTrue(js.job_available())

        js.add_result(None)
        job4 = js.get_job()

        self.assertFalse(js.is_done())
        self.assertFalse(js.job_available())

        js.add_result(None)

        self.assertTrue(js.is_done())
        self.assertFalse(js.job_available())
        self.assertTrue(js.results().is_complete())
        self.assertEqual(len(js.results()), 3)

    def test_done_means_done(self):
        """
        Ensures that when a job set is done, jobs cannot be returned, results
        cannot be added, and the job can no longer be cancelled.
        """

        js = jobs.JobSet(range(0), loop=self._loop)

        with self.assertRaises(RuntimeError):
            js.return_job(None)
        with self.assertRaises(RuntimeError):
            js.add_result(None)
        with self.assertRaises(RuntimeError):
            js.cancel()

    def test_cancel(self):
        """
        Ensures that cancelling a job set instantly finishes it, allowing no
        more jobs to be retrieved.
        """

        js = jobs.JobSet(range(3), loop=self._loop)

        job1 = js.get_job()
        job2 = js.get_job()
        js.return_job(job1)

        self.assertFalse(js.is_done())
        self.assertTrue(js.job_available())

        js.cancel()

        self.assertTrue(js.is_done())
        self.assertFalse(js.job_available())


if __name__ == "__main__":
    unittest.main()

