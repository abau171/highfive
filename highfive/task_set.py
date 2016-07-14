import asyncio
import collections


class TaskSetClosed(Exception):
    pass


class TaskSetProcess:

    def __init__(self, ts):
        self._task_iterator = iter(ts)
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

