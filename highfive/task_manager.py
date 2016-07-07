import threading
import collections
import contextlib

import highfive.distributed_process


NO_RESULT = object()


class Task:

    def __init__(self):
        self._done = False
        self._result = NO_RESULT

    def done(self, result=NO_RESULT):
        self._result = result
        self._done = True

    def is_done(self):
        return self._done

    def run(self, connection):
        raise NotImplementedError

    def get_result(self):
        return self._result


class TaskManager:

    def __init__(self):

        self._mutex = threading.Lock()
        self._queue_update = threading.Condition(self._mutex)

        self._process_queue = collections.deque()

    def add_process(self, process):
        with self._queue_update:
            self._process_queue.append(process)
            self._queue_update.notify()

    @contextlib.contextmanager
    def task(self):
        process = None
        task = None
        with self._queue_update:
            while task is None:
                while len(self._process_queue) == 0:
                    self._queue_update.wait()
                process = self._process_queue[0]
                try:
                    task = process.next_task()
                except highfive.distributed_process.Closed:
                    self._process_queue.popleft()
            self._queue_update.notify()
        try:
            yield task
        finally:
            # No need to lock here, not using the queue and the process locks itself.
            # TODO Move task report result / requeue logic to DistributedProcess?
            if task.is_done():
                process.task_done(task.get_result())
            else:
                process.requeue_task(task)

