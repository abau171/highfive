import random
import asyncio

import jobs
import manager


class AddJob(jobs.Job):
    def __init__(self, a, b):
        self._a = a
        self._b = b
    def get_call(self):
        #print("GET: {} + {}".format(self._a, self._b))
        return [self._a, self._b]
    def get_result(self, response):
        #print("RESULT: {}".format(response))
        return self._a, self._b, response


class AddWorker:
    def __init__(self, loop):
        self._loop = loop
    async def make_call(self, call):
        await asyncio.sleep(random.random(), loop=self._loop)
        #print("WORKER: {} + {} = {}".format(call[0], call[1], call[0] + call[1]))
        return call[0] + call[1]
    def close(self):
        pass


async def main(loop):

    m = manager.JobManager(loop=loop)

    js = jobs.JobSet((AddJob(i, i * i) for i in range(10)), loop=loop)
    ri = jobs.ResultSetIterator(js.results())
    m.add_job_set(js)

    m.add_worker(AddWorker(loop))
    m.add_worker(AddWorker(loop))

    try:
        while True:
            a, b, c = await ri.next_result()
            print("{} + {} = {}".format(a, b, c))
            if a == 5:
                js.cancel()
    except jobs.EndOfResults:
        pass

    js = jobs.JobSet((AddJob(i, i * i) for i in range(10)), loop=loop)
    ri = jobs.ResultSetIterator(js.results())
    m.add_job_set(js)
    js2 = jobs.JobSet((AddJob(i, i * i) for i in range(10)), loop=loop)
    m.add_job_set(js2)

    try:
        while True:
            a, b, c = await ri.next_result()
            print("{} + {} = {}".format(a, b, c))
            if a == 5:
                m.close()
    except jobs.EndOfResults:
        pass

    await m.wait_closed()


loop = asyncio.get_event_loop()
asyncio.set_event_loop(None)
loop.run_until_complete(main(loop))
loop.close()

