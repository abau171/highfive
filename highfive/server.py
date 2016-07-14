import collections
import json
import asyncio

from . import task_set


class Task:

    def prepare(self):
        raise NotImplementedError

    def finish(self, result_struct):
        return result_struct


def encode_task(task):
    if isinstance(task, Task):
        task_struct = task.prepare()
    else:
        task_struct = task
    task_json = json.dumps(task_struct) + "\n"
    task_msg = task_json.encode("utf-8")
    return task_msg


def decode_result(task, result_msg):
    result_json = result_msg.decode("utf-8")
    result_struct = json.loads(result_json)
    if isinstance(task, Task):
        result = task.finish(result_struct)
    else:
        result = result_struct
    return result


class MasterServer:

    def __init__(self, loop):
        self._loop = loop
        self._server = None
        self._closing = False
        self._clients = set()
        self._waiters = collections.deque()
        self._task_set_queue = task_set.TaskSetProcessQueue()

    async def _handle(self, reader, writer):
        try:
            while not self._closing:
                ts, task = await self._task_set_queue.next_task()
                try:
                    task_msg = encode_task(task)
                    writer.write(task_msg)
                    result_msg = await reader.readline()
                    result = decode_result(task, result_msg)
                    ts.task_done(result)
                except:
                    ts.return_task(task)
                    raise
        except task_set.TaskSetQueueClosed:
            pass
        except json.JSONDecodeError:
            pass
        finally:
            writer.close()

    async def _accept(self, reader, writer):
        future = asyncio.ensure_future(
            self._handle(reader, writer), loop=self._loop)
        def client_done(future):
            self._clients.remove(future)
        future.add_done_callback(client_done)
        self._clients.add(future)

    async def start(self, hostname, port):
        self._server = await asyncio.streams.start_server(
            self._accept, hostname, port, loop=self._loop)

    async def stop(self):
        self._task_set_queue.close()
        self._closing = True
        self._server.close()
        await self._server.wait_closed()
        if len(self._clients) != 0:
            await asyncio.wait(self._clients)
        while len(self._waiters) > 0:
            self._waiters.popleft().set_result(None)

    async def wait_stopped(self):
        waiter = asyncio.Future(loop=self._loop)
        self._waiters.append(waiter)
        await waiter

    async def run_task_set(self, tasks):
        return self._task_set_queue.run_task_set(tasks)


async def start_master_server(hostname, port, loop):
    server = MasterServer(loop)
    await server.start(hostname, port)
    return server

