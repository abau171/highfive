import asyncio
import threading

from . import Master


def run_master(master, loop):

    asyncio.set_event_loop(None)
    loop.run_until_complete(master.wait_closed())


class MasterDaemon:

    def __init__(self, host="", port=48484):

        self._host = host
        self._port = port

        self._loop = asyncio.new_event_loop()
        self._master = Master(host, port, loop=self._loop)

    def __enter__(self):

        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):

        self.close()
        return self.wait_closed()

    def start(self):

        self._loop.run_until_complete(self._master.start())
        self._thread = threading.Thread(
                target=run_master, args=(self._master, self._loop))
        self._thread.start()

    def close(self):

        self._loop.call_soon_threadsafe(self._master.close)

    def wait_closed(self):

        asyncio.run_coroutine_threadsafe(self._master.wait_closed(),
                loop=self._loop).result()

    async def _run(self, job_iterable):

        return self._master.run(job_iterable)

    def run(self, job_iterable):

        js = asyncio.run_coroutine_threadsafe(self._run(job_iterable),
                loop=self._loop).result()
        return JobSetWrapper(js, loop=self._loop)


class JobSetWrapper:

    def __init__(self, js, *, loop):

        self._js = js
        self._loop = loop

    async def _results(self):

        return self._js.results()

    def __enter__(self):

        return self

    def __exit__(self, exc_type, exc, tb):

        self.cancel()
        self.wait_done()

    def results(self):

        results = asyncio.run_coroutine_threadsafe(self._results(),
                loop=self._loop).result()
        return ResultSetIteratorWrapper(results, loop=self._loop)

    def cancel(self):

        self._loop.call_soon_threadsafe(self._js.cancel)

    def wait_done(self):

        asyncio.run_coroutine_threadsafe(self._js.wait_done(),
                loop=self._loop).result()


class ResultSetIteratorWrapper:

    def __init__(self, results, *, loop):

        self._results = results
        self._loop = loop

    def __iter__(self):

        return self

    def __next__(self):

        try:
            r = asyncio.run_coroutine_threadsafe(self._results.__anext__(),
                loop=self._loop).result()
        except StopAsyncIteration:
            raise StopIteration
        return r

