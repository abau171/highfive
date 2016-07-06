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


class TaskSetInProgress(Exception):
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


class TaskSet:

    def __init__(self, task_iterable):
        self.task_queue = TaskQueue(task_iterable)
        self.result_queue = collections.deque()
        self.cancelled = False

    def has_next_task(self):
        return self.task_queue.has_next()

    def next_task(self):
        return self.task_queue.next()

    def task_done(self):
        self.task_queue.task_done()

    def return_task(self, task):
        self.task_queue.return_task(task)

    def has_unfinished_tasks(self):
        return self.task_queue.has_unfinished_tasks()

    def has_results(self):
        return len(self.result_queue) > 0

    def put_result(self, result):
        self.result_queue.append(result)

    def pop_result(self):
        return self.result_queue.pop()

    def cancel(self):
        self.task_queue.clear()
        self.result_queue.clear()
        self.cancelled = True

    def is_cancelled(self):
        return self.cancelled

    def is_done(self):
        return not self.has_results() and not self.has_unfinished_tasks()


class TaskManager:

    def __init__(self):
        self.task_set = None
        self.mutex = threading.Lock()
        self.tasks_available = threading.Condition(self.mutex)
        self.results_available = threading.Condition(self.mutex)

    @contextlib.contextmanager
    def _activate(self, task_iterable):
        with self.mutex:
            if self.task_set is not None:
                raise TaskSetInProgress
            self.task_set = TaskSet(task_iterable)
            self.tasks_available.notify()
        try:
            yield
        finally:
            with self.mutex:
                self.task_set = None

    def process(self, task_iterable):
        with self._activate(task_iterable):
            while True:
                with self.mutex:
                    if self.task_set.is_done():
                        break
                    while not self.task_set.has_results():
                        self.results_available.wait()
                    result = self.task_set.pop_result()
                if result is not NO_RESULT:
                    yield result

    def cancel_process(self):
        with self.mutex:
            if self.task_set is not None:
                self.task_set.cancel()

    def _get_next_task(self):
        with self.mutex:
            while self.task_set is None or not self.task_set.has_next_task():
                self.tasks_available.wait()

            task = self.task_set.next_task()
            if self.task_set.has_next_task():
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
                    self.task_set.task_done()
                    self.task_set.put_result(task.get_result())
                    self.results_available.notify()
                elif not self.task_set.is_cancelled():
                    self.task_set.return_task(task)
                    self.tasks_available.notify()
                else:
                    self.task_set.task_done()

