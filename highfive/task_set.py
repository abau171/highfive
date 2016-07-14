import asyncio
import collections


class TaskSetClosed(Exception):
    pass


class TaskSetQueueClosed(Exception):
    pass


class TaskSetProcess:

    def __init__(self, tasks):
        self._task_iterator = iter(tasks)
        self._active_tasks = 0

        self._load_next_task()

        self._returned_tasks = collections.deque()
        self._closed = False
        self._getters = collections.deque()

        self._close_if_no_tasks()

    def _load_next_task(self):
        try:
            self._on_deck = next(self._task_iterator)
            self._active_tasks += 1
        except StopIteration:
            self._on_deck = None

    def _close_if_no_tasks(self):
        if self._active_tasks == 0:
            self.close()

    async def next_task(self):
        if self._closed:
            raise TaskSetClosed()
        if len(self._returned_tasks) > 0:
            return self._returned_tasks.popleft()
        elif self._on_deck is not None:
            task = self._on_deck
            self._load_next_task()
            return task
        else:
            getter = asyncio.Future()
            self._getters.append(getter)
            return await getter

    def return_task(self, task):
        if self._closed:
            return
        if len(self._getters) > 0:
            self._getters.popleft().set_result(task)
        else:
            self._returned_tasks.append(task)

    def task_done(self):
        self._active_tasks -= 1
        self._close_if_no_tasks()

    def close(self):
        self._closed = True
        while len(self._getters) > 0:
            self._getters.pop().set_exception(TaskSetClosed())

    def is_closed(self):
        return self._closed


class TaskSetProcessQueue:

    def __init__(self):
        self._queue = collections.deque()
        self._cur_task_set = None
        self._waiters = collections.deque()
        self._closed = False

    def run_task_set(self, tasks):
        if self._closed:
            raise TaskSetQueueClosed()
        ts = TaskSetProcess(tasks)
        if self._cur_task_set is None:
            self._cur_task_set = ts
            while len(self._waiters) > 0:
                self._waiters.popleft().set_result(None)
        else:
            self._queue.append(ts)
        return ts

    async def next_task(self):
        while not self._closed:
            while self._cur_task_set is None:
                waiter = asyncio.Future()
                self._waiters.append(waiter)
                await waiter
            ts = self._cur_task_set
            try:
                task = await ts.next_task()
                return ts, task
            except TaskSetClosed:
                if self._cur_task_set is ts:
                    if len(self._queue) > 0:
                        self._cur_task_set = self._queue.popleft()
                    else:
                        self._cur_task_set = None
        else:
            raise TaskSetQueueClosed()

    def close(self):
        self._closed = True
        self._queue.clear()
        if self._cur_task_set is not None:
            self._cur_task_set.close()
            self._cur_task_set = None
        while len(self._waiters) > 0:
            self._waiters.popleft().set_exception(TaskSetQueueClosed())

