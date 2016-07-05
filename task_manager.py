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


class TaskManager:

    def __init__(self):
        self.process_active = False
        self.task_iterator = None
        self.next_task = None
        self.return_queue = None
        self.result_queue = None
        self.unfinished_tasks = 0
        self.cancelled = False

        self.mutex = threading.Lock()
        self.tasks_available = threading.Condition(self.mutex)
        self.results_available = threading.Condition(self.mutex)

    def process(self, task_iterable):
        with self.mutex:
            if self.process_active:
                raise ProcessActive
            self.process_active = True

        self.task_iterator = iter(task_iterable)
        try:
            self.next_task = next(task_iterator)
        except StopIteration:
            self.task_iterator = None
            return
        self.return_queue = collections.deque()
        self.result_queue = collections.deque()
        self.unfinished_tasks = 1
        self.cancelled = False

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

        self.task_iterator = None
        self.return_queue = None
        self.result_queue = None
        self.cancelled = False

        with self.mutex:
            self.process_active = False

    def cancel_process(self):
        with self.mutex:
            if self.process_active:
                self.unfinished_tasks -= len(self.return_queue)
                self.return_queue.clear()
                if self.next_task is not None:
                    self.next_task = None
                    self.unfinished_tasks -= 1
                self.cancelled = True

    def _get_next_task(self):
        with self.mutex:
            while self.next_task is None and len(self.return_queue) == 0:
                self.tasks_available.wait()

            if len(self.return_queue) > 0:
                task = self.return_queue.pop()
            else:
                task = self.next_task
                if self.cancelled:
                    self.next_task = None
                else:
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
                self.unfinished_tasks -= 1
                if task.is_done():
                    self.result_queue.append(task.get_result())
                    self.results_available.notify()
                elif not self.cancelled:
                    self.return_queue.append(task)
                    self.unfinished_tasks += 1
                    self.tasks_available.notify()

