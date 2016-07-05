import queue
import contextlib

import iter_queue


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


def all_in_queue(queue):
    while queue.qsize() > 0:
        yield queue.get()


class TaskManager:

    def __init__(self):
        self.task_queue = iter_queue.IterableQueue()
        self.result_queue = queue.Queue()

    def process(self, task_iterable):
        self.task_queue.put_iterable(task_iterable)
        self.task_queue.join()
        results = list(all_in_queue(self.result_queue))
        return results

    @contextlib.contextmanager
    def handle_task(self):
        task = self.task_queue.get()
        try:
            yield task
        finally:
            if task.is_done():
                if task.get_result() is not NO_RESULT:
                    self.result_queue.put(task.get_result())
            else:
                self.task_queue.put(task)
            self.task_queue.task_done()

