import threading
import collections


class Closed(Exception):
    pass


class TaskQueue:

    def __init__(self, mutex, task_iterable):

        self._task_update = threading.Condition(mutex)

        self._task_iterator = iter(task_iterable)
        self._open_tasks = 0
        self._requeue = collections.deque()
        self._closed = False

        self._load_next()
        self._close_if_no_tasks()

    def _load_next(self):
        try:
            self.on_deck = next(self._task_iterator)
            self._open_tasks += 1
        except StopIteration:
            self.on_deck = None

    def _has_next(self):
        return self.on_deck is not None or len(self._requeue) > 0

    def next(self):

        while not self._has_next() and not self._closed:
            self._task_update.wait()

        if self._closed:
            raise Closed
        else: # must have the next task available
            if len(self._requeue) > 0:
                task = self._requeue.popleft()
            elif self.on_deck is not None:
                task = self.on_deck
                self._load_next()
            if self._has_next():
                self._task_update.notify()
            return task

    def push(self, task):
        if not self._closed:
            self._requeue.append(task)

    def _close_if_no_tasks(self):
        if self._open_tasks == 0:
            self._close()

    def _close(self):
        self._closed = True
        self._task_update.notify_all()

    def task_done(self):
        if not self._closed:
            self._open_tasks -= 1
            self._close_if_no_tasks()

    def close(self):
        if not self._closed:
            self._close()

    def is_closed(self):
        return self._closed


class ResultSet:

    def __init__(self, mutex):

        self._results_update = threading.Condition(mutex)

        self._results = []
        self._ended = False

    def add(self, result):
        if not self._ended:
            self._results.append(result)
            self._results_update.notify_all()

    def end_of_results(self):
        if not self._ended:
            self._ended = True
            self._results_update.notify_all()

    def __iter__(self):
        i = 0
        while True:
            # possible need for a reentrant mutex if __iter__ is ever called in DistributedProcess
            with self._results_update:
                while i >= len(self._results) and not self._ended:
                    self._results_update.wait()
                if i < len(self._results):
                    result = self._results[i]
                else: # must be at absolute end of results
                    break
            # yield outside mutex lock (result is going to the user)
            yield result
            i += 1


class DistributedProcess:

    def __init__(self, task_iterable):

        self._mutex = threading.Lock()

        self._task_queue = TaskQueue(task_iterable)
        self._results = ResultSet(self._mutex)

        self._end_results_if_task_queue_closed()

    # Somehow, TaskQueue and ResultSet are bound to the same context.
    # Specifically, the context contains the mutex and close functions they use.
    # This context should be explicit somehow, but this is a simple workaround.
    def _end_results_if_task_queue_closed(self):
        if self._task_queue.is_closed():
            self._results.end_of_results()

    def results(self):
        # IMPORTANT: Do not lock on self._mutex, or deadlock will occur! (uses self._mutex internally)
        return iter(self._results)

    def close(self):
        with self._mutex:
            self._task_queue.close()
            self._results.end_of_results()

    def next_task(self):
        with self._mutex:
            return self._task_queue.next()

    def task_done(self, result):
        with self._mutex:
            self._results.add(result)
            self._task_queue.task_done()
            self._end_results_if_task_queue_closed()

    def requeue_task(self, task):
        with self._mutex:
            self._task_queue.push(task)

