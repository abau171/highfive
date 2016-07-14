import threading
import asyncio

from . import server


def run_master_thread(server, loop):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(server.wait_stopped())
    loop.close()


class Master:

    def __init__(self, hostname, port):
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

    def run_task_set(self, ts):
        return self._run_coro(self._server.run_task_set(ts))

