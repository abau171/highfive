import threading
import collections
import contextlib


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


class ProcessActive(Exception):
    pass


class Empty(Exception):
    pass


class TaskQueue:

    def __init__(self, task_iterable):
        self.task_iterator = iter(task_iterable)
        try:
            self.next_task = next(self.task_iterator)
            self.unfinished_tasks = 1
        except StopIteration:
            self.next_task = None
            self.unfinished_tasks = 0
        self.return_queue = collections.deque()

    def next(self):
        if len(self.return_queue) > 0:
            return self.return_queue.pop()
        elif self.next_task is not None:
            task = self.next_task
            try:
                self.next_task = next(self.task_iterator)
                self.unfinished_tasks += 1
            except StopIteration:
                self.next_task = None
            return task
        else:
            raise Empty

    def has_next(self):
        return len(self.return_queue) > 0 or self.next_task is not None

    def clear(self):
        if self.next_task is not None:
            self.next_task = None
            self.unfinished_tasks -= 1
        self.unfinished_tasks -= len(self.return_queue)
        self.return_queue.clear()

    def task_done(self):
        self.unfinished_tasks -= 1

    def has_unfinished_tasks(self):
        return self.unfinished_tasks > 0

    def return_task(self, task):
        self.return_queue.append(task)


class TaskManager:

    def __init__(self):
        self.process_active = False
        self.task_queue = None
        self.result_queue = None
        self.cancelled = False

        self.mutex = threading.Lock()
        self.tasks_available = threading.Condition(self.mutex)
        self.results_available = threading.Condition(self.mutex)

    def process(self, task_iterable):
        try:

            with self.mutex:
                if self.process_active:
                    raise ProcessActive
                self.process_active = True

            self.task_queue = TaskQueue(task_iterable)
            if not self.task_queue.has_next():
                self.task_queue = None
                return
            self.result_queue = collections.deque()
            self.cancelled = False

            with self.mutex:
                self.tasks_available.notify()

            while True:
                with self.mutex:
                    if len(self.result_queue) == 0 and not self.task_queue.has_unfinished_tasks():
                        break
                    while len(self.result_queue) == 0:
                        self.results_available.wait()
                    result = self.result_queue.pop()
                if result is not NO_RESULT:
                    yield result

        finally:

            self.task_queue = None
            self.result_queue = None
            self.cancelled = False

            with self.mutex:
                self.process_active = False

    def cancel_process(self):
        with self.mutex:
            if self.process_active:
                self.task_queue.clear()
                self.cancelled = True

    def _get_next_task(self):
        with self.mutex:
            while self.task_queue is None or not self.task_queue.has_next():
                self.tasks_available.wait()

            task = self.task_queue.next()
            if self.task_queue.has_next():
                self.tasks_available.notify()
            return task

    @contextlib.contextmanager
    def handle_task(self):
        task = self._get_next_task()
        try:
            yield task
        finally:
            with self.mutex:
                if task.is_done():
                    self.task_queue.task_done()
                    self.result_queue.append(task.get_result())
                    self.results_available.notify()
                elif not self.cancelled:
                    self.task_queue.return_task(task)
                    self.tasks_available.notify()
                else:
                    self.task_queue.task_done()

