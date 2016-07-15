import threading
import asyncio

from . import server
from . import task_set


class TaskSetUserView:

    def __init__(self, ts, loop):
        self._ts = ts
        self._loop = loop
        self._results = []
        self._end_of_results_reached = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cancel()

    def _run_coro(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    def next_result(self):
        try:
            result = self._run_coro(self._ts.next_result())
            self._results.append(result)
            return result
        except task_set.EndOfResults:
            self._end_of_results_reached = True
            raise

    def results(self):
        i = 0
        while True:
            if i < len(self._results):
                yield self._results[i]
            elif not self._end_of_results_reached:
                try:
                    yield self.next_result()
                except task_set.EndOfResults:
                    break
            else:
                break
            i += 1

    def cancel(self):
        self._loop.call_soon_threadsafe(self._ts.close)


def run_master_thread(server, loop):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(server.wait_stopped())
    loop.close()


class Master:

    def __init__(self, hostname="", port=48484):
        self._loop = asyncio.new_event_loop()
        self._server = self._loop.run_until_complete(
            server.start_master_server(hostname, port, self._loop))
        self._thread = threading.Thread(
            target=run_master_thread, args=(self._server, self._loop))
        self._thread.start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def _run_coro(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    def stop(self):
        self._run_coro(self._server.stop())
        self._thread.join()
        self._loop.close()

    def run_task_set(self, tasks):
        ts = self._run_coro(self._server.run_task_set(tasks))
        return TaskSetUserView(ts, self._loop)

