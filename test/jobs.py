import asyncio
import unittest

import highfive.jobs as jobs


class TestResultSet(unittest.TestCase):

    def setUp(self):

        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def test_no_results(self):
        """
        Ensures that a new result set has no results and is incomplete, then
        ensures that completion of an empty result set works.
        """

        rs = jobs.ResultSet(loop=self._loop)

        self.assertEqual(len(rs), 0)
        self.assertFalse(rs.is_complete())
        
        rs.complete()

        self.assertEqual(len(rs), 0)
        self.assertTrue(rs.is_complete())

        self._loop.close()

    def test_results(self):
        """
        Ensures that a result set can add results, and effectively tracks and
        returns them before a proper result set completion.
        """

        rs = jobs.ResultSet(loop=self._loop)

        rs.add(0)

        self.assertEqual(len(rs), 1)
        self.assertFalse(rs.is_complete())

        rs.add(1)
        rs.add(2)

        self.assertEqual(len(rs), 3)
        self.assertFalse(rs.is_complete())

        self.assertEqual(rs[0], 0)
        self.assertEqual(rs[1], 1)
        self.assertEqual(rs[2], 2)

        rs.complete()

        self.assertEqual(len(rs), 3)
        self.assertTrue(rs.is_complete())

        self._loop.close()

    def test_ops_after_complete(self):
        """
        Completes a result set, then makes sure new results cannot be added
        and it cannot be completed again.
        """

        rs = jobs.ResultSet(loop=self._loop)

        rs.complete()

        with self.assertRaises(RuntimeError):
            rs.add(0)
        with self.assertRaises(RuntimeError):
            rs.complete()

        self._loop.close()

    def test_bad_index(self):
        """
        Ensures that fetching a result using an improper index fails.
        """

        rs = jobs.ResultSet(loop=self._loop)

        rs.add(0)
        rs.add(1)

        with self.assertRaises(IndexError):
            rs[2]
        with self.assertRaises(IndexError):
            rs[3]
        with self.assertRaises(IndexError):
            rs[10]

        self._loop.close()

    def test_wait_complete(self):
        """
        Ensures that the completion of a result set invokes a change, and that
        all future attempts to wait for a change return immediately.
        """

        rs = jobs.ResultSet(loop=self._loop)

        async def completer():
            rs.complete()

        async def waiter():
            c = self._loop.create_task(completer())
            await rs.wait_changed()
            self.assertTrue(rs.is_complete())
            await rs.wait_changed()

        self._loop.run_until_complete(waiter())

        self._loop.close()

    def test_wait_result_add(self):
        """
        Ensures that adding a result invokes a change.
        """

        rs = jobs.ResultSet(loop=self._loop)

        async def adder():
            rs.add(None)

        async def waiter():
            c = self._loop.create_task(adder())
            await rs.wait_changed()
            self.assertEqual(len(rs), 1)
            self.assertFalse(rs.is_complete())

        self._loop.run_until_complete(waiter())

        self._loop.close()


class TestResultSetIterator(unittest.TestCase):

    def setUp(self):

        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def test_no_results(self):
        """
        Ensures that a completed result set with no results immediately
        indicates the end of results when waited on.
        """

        rs = jobs.ResultSet(loop=self._loop)
        rs.complete()

        ri = jobs.ResultSetIterator(rs)

        async def nexter():
            with self.assertRaises(jobs.EndOfResults):
                await ri.next_result()

        self._loop.run_until_complete(nexter())

        self._loop.close()

    def test_some_results(self):
        """
        Ensures that results can be iterated over asynchronously whether
        results are currently available or not.
        """

        rs = jobs.ResultSet(loop=self._loop)
        rs.add(0)

        ri = jobs.ResultSetIterator(rs)

        async def add_completer():
            rs.add(1)
            rs.complete()

        async def nexter():
            await ri.next_result()
            self._loop.create_task(add_completer())
            await ri.next_result()
            with self.assertRaises(jobs.EndOfResults):
                await ri.next_result()

        self._loop.run_until_complete(nexter())

        self._loop.close()


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

        self._loop.close()

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

        self._loop.close()

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

        self._loop.close()

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

        self._loop.close()

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

        self._loop.close()

    def test_wait_done(self):
        """
        Ensures that waiting on the completion of a job set works whether the
        job set is running or already complete.
        """

        js = jobs.JobSet(range(1), loop=self._loop)

        async def completer():
            job = js.get_job()
            js.add_result(None)

        async def waiter():
            c = self._loop.create_task(completer())
            await js.wait_done()
            self.assertTrue(js.is_done())
            await js.wait_done()

        self._loop.run_until_complete(waiter())

        self._loop.close()


if __name__ == "__main__":
    unittest.main()

