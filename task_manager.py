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


class TaskManager:

    def __init__(self):
        self.task_iterator = None
        self.next_task = None
        self.return_queue = None
        self.result_queue = None
        self.unfinished_tasks = None

        self.mutex = threading.Lock()
        self.tasks_available = threading.Condition(self.mutex)
        self.results_available = threading.Condition(self.mutex)

    def process(self, task_iterable):
        self.task_iterator = iter(task_iterable)
        try:
            self.next_task = next(task_iterable)
        except StopIteration:
            self.task_iterator = None
            return
        self.return_queue = collections.deque()
        self.result_queue = collections.deque()
        self.unfinished_tasks = 1

        with self.mutex:
            self.tasks_available.notify()

        while True:
            with self.mutex:
                if len(self.result_queue) == 0 and self.unfinished_tasks == 0:
                    break
                while len(self.result_queue) == 0:
                    self.results_available.wait()
                result = self.result_queue.pop()
            if result is not NO_RESULT:
                yield result

    def _get_next_task(self):
        with self.mutex:
            while self.next_task is None and len(self.return_queue) == 0:
                self.tasks_available.wait()

            if len(self.return_queue) > 0:
                task = self.return_queue.pop()
            else:
                task = self.next_task
                try:
                    self.next_task = next(self.task_iterator)
                    self.unfinished_tasks += 1
                    self.tasks_available.notify()
                except StopIteration:
                    self.next_task = None

            return task

    @contextlib.contextmanager
    def handle_task(self):
        task = self._get_next_task()
        try:
            yield task
        finally:
            with self.mutex:
                if task.is_done():
                    self.result_queue.append(task.get_result())
                    self.results_available.notify()
                    self.unfinished_tasks -= 1
                else:
                    self.return_queue.append(task)
                    self.tasks_available.notify()

