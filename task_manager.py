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

    def task_done(self, result):
        self.task_queue.task_done()
        if not self.cancelled:
            self.result_queue.append(result)

    def return_task(self, task):
        if not self.cancelled:
            self.task_queue.return_task(task)
        else:
            self.task_queue.task_done()

    def has_unfinished_tasks(self):
        return self.task_queue.has_unfinished_tasks()

    def has_results(self):
        return len(self.result_queue) > 0

    def pop_result(self):
        return self.result_queue.pop()

    def cancel(self):
        if not self.cancelled:
            self.task_queue.clear()
            self.result_queue.clear()
            self.cancelled = True

    def is_done(self):
        return not self.has_results() and not self.has_unfinished_tasks()


class NoResult:
    pass


class DeadController:
    pass


class TaskSetController:

    def __init__(self, result_iterator, cancel_function):
        self._result_iterator = result_iterator
        self._cancel_function = cancel_function
        self._dead = False

    def _kill(self):
        self._dead = True

    def results(self):
        return self._result_iterator

    def next_result(self):
        try:
            result = next(self._result_iterator)
            return result
        except StopIteration:
            raise NoResult

    def cancel(self):
        if self._dead:
            raise DeadController
        self._cancel_function()


class TaskManager:

    def __init__(self):
        self.task_set = None
        self.mutex = threading.Lock()
        self.tasks_available = threading.Condition(self.mutex)
        self.results_available = threading.Condition(self.mutex)

    def _task_set_results(self):
        while True:
            with self.mutex:
                while not (self.task_set.is_done() or self.task_set.has_results()):
                    self.results_available.wait()
                if self.task_set.is_done():
                    break
                result = self.task_set.pop_result()
            if result is not NO_RESULT:
                yield result

    def _cancel_task_set(self):
        with self.mutex:
            self.task_set.cancel()

    @contextlib.contextmanager
    def run_task_set(self, task_iterable):
        with self.mutex:
            if self.task_set is not None:
                raise TaskSetInProgress
            self.task_set = TaskSet(task_iterable)
            self.tasks_available.notify()
        result_iterator = self._task_set_results()
        controller = TaskSetController(result_iterator, self._cancel_task_set)
        try:
            yield controller
        finally:
            controller._kill()
            self._cancel_task_set()
            for _ in result_iterator: # easy way of making sure all tasks are done
                pass
            with self.mutex:
                self.task_set = None

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
                    self.task_set.task_done(task.get_result())
                    self.results_available.notify()
                else:
                    self.task_set.return_task(task)
                    self.tasks_available.notify()
                    self.results_available.notify() # if cancelled, the task is considered done which influences results

