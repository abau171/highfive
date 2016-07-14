import asyncio
import collections


class TaskSetClosed(Exception):
    pass


class TaskSetProcess:

    def __init__(self, ts):
        self._task_iterator = iter(ts)
        self._load_next_task()
        self._returned_tasks = collections.deque()
        self._closed = False

        self._getters = collections.deque()

    def _load_next_task(self):
        try:
            self._on_deck = next(self._task_iterator)
        except StopIteration:
            self._on_deck = None

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
            future = asyncio.Future()
            self._getters.append(future)
            return await future

    def return_task(self, task):
        if self._closed:
            return
        if len(self._getters) > 0:
            self._getters.popleft().set_result(task)
        else:
            self._returned_tasks.append(task)

    def cancel(self):
        self._closed = True
        while len(self._getters) > 0:
            self._getters.pop().set_exception(TaskSetClosed())

