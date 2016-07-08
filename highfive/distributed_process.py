"""Thread-safe distributed process state."""

import threading
import collections


class Closed(Exception):
    """Exception raised when the task queue will never return a task."""
    pass


class TaskQueue:
    """A queue of tasks which also tracks the state of their execution.

    The queue dynamically adds elements from an iterable source which provides
    the task objects in the queue. For each task removed, either the queue
    should eventually be informed when the task has been completed or the task
    should be returned to the queue if it could not be executed.

    When all tasks have been completed, the queue will close itself and no
    longer return tasks. The queue can also be closed manually before all
    tasks are completed.

    All operations are expected to be called with the mutex used to initialize
    the queue locked.

    """

    def __init__(self, mutex, task_iterable):

        self._task_update = threading.Condition(mutex)

        self._task_iterator = iter(task_iterable)
        self._open_tasks = 0
        self._requeue = collections.deque()
        self._closed = False

        self._load_next()
        self._close_if_no_tasks()

    def _load_next(self):
        """Loads the next task from the task iterator."""

        try:
            self._on_deck = next(self._task_iterator)
            self._open_tasks += 1
        except StopIteration:
            self._on_deck = None

    def _has_next(self):
        """Determines whether a task is currently available to be returned."""

        return self._on_deck is not None or len(self._requeue) > 0

    def next(self):
        """Returns a task from the queue.

        This method blocks until either a task becomes available or the queue
        is closed. The Closed exception is raised if this method is called
        after the queue has been closed or if it is closed while this method
        is waiting for a task to return.

        """

        while not self._has_next() and not self._closed:
            self._task_update.wait()

        if self._closed:
            raise Closed
        else: # Next task must be available.
            if len(self._requeue) > 0:
                task = self._requeue.popleft()
            elif self._on_deck is not None:
                task = self._on_deck
                self._load_next()
            if self._has_next():
                self._task_update.notify()
            return task

    def push(self, task):
        """Requeues an incomplete task."""

        if not self._closed:
            self._requeue.append(task)
            self._task_update.notify()

    def _close_if_no_tasks(self):
        """Closes the queue if all tasks have been completed."""

        if self._open_tasks == 0:
            self._close()

    def _close(self):
        """Closes the queue."""

        self._closed = True
        self._task_update.notify_all()

    def task_done(self):
        """Informs the queue that a task has been completed.

        The queue may automatically close after this method is called.

        """

        if not self._closed:
            self._open_tasks -= 1
            self._close_if_no_tasks()

    def close(self):
        """Public-facing close method."""

        if not self._closed:
            self._close()

    def is_closed(self):
        """Determines whether the queue is closed."""

        return self._closed


class ResultSet:
    """Set of results which can be iterated over in multiple threads.

    The iterating over the results may block until more results are available
    or the end of the results is reached. Results cannot be added after the
    end of the results has been reached.

    All operations are expected to be called with the mutex used to initialize
    the result set locked.

    """

    def __init__(self, mutex):

        self._results_update = threading.Condition(mutex)

        self._results = []
        self._ended = False

    def add(self, result):
        """Adds a new result to the result set."""

        if not self._ended:
            self._results.append(result)
            self._results_update.notify_all()

    def end_of_results(self):
        """Indicates that no more results will be added in the future.

        After this method is called, iterating over the result set will no
        longer block and will reach the end of the iterator.

        """

        if not self._ended:
            self._ended = True
            self._results_update.notify_all()

    def __iter__(self):
        """Iterates over the result set, waiting for results to be available.

        Iteration will not stop until the end of results is indicated.

        """

        i = 0
        while True:
            # Possible need for a reentrant mutex if __iter__ is ever called
            # in DistributedProcess.
            with self._results_update:
                while i >= len(self._results) and not self._ended:
                    self._results_update.wait()
                if i < len(self._results):
                    result = self._results[i]
                else: # Must be at absolute end of results.
                    break
            # Yield outside mutex lock (result is going to the user).
            yield result
            i += 1


class DistributedProcess:
    """Encapsulates the state of an entire distributed process.

    The process queues tasks and records the results of their execution. Any
    task being executed must indicate that it was completed or be returned
    to the process to be executed again. All results found are recorded and
    can be iterated over. The process can close itself when all tasks have
    been completed or can be manually closed early. Manually closing the
    process locks the results and no new results will be recorded. Results can
    be iterated over even after the process has closed.

    All operations on the process are thread-safe.

    """

    def __init__(self, task_iterable):

        self._mutex = threading.Lock()

        self._task_queue = TaskQueue(self._mutex, task_iterable)
        self._results = ResultSet(self._mutex)

        self._end_results_if_task_queue_closed()

    # Somehow, TaskQueue and ResultSet are bound to the same context.
    # The context contains the mutex and close functions they use.
    # This context should be explicit, but this is a simple workaround.
    def _end_results_if_task_queue_closed(self):
        """Locks the results if the task queue is closed."""

        if self._task_queue.is_closed():
            self._results.end_of_results()

    def results(self):
        """Iterates over the results of the process.

        This iterator may block until new results are found, but will attempt
        to report results as soon as they are found.

        """

        # Do not lock on self._mutex, or deadlock will occur!
        # Uses self._mutex internally.
        return iter(self._results)

    def close(self):
        """Manually closes the process."""

        with self._mutex:
            self._task_queue.close()
            self._results.end_of_results()

    def next_task(self):
        """Gets the next class to be executed for the process."""

        with self._mutex:
            return self._task_queue.next()

    def task_done(self, result):
        """Indicates that a task has been completed and reports the result."""

        with self._mutex:
            self._results.add(result) # TODO Check if result exists first.
            self._task_queue.task_done()
            self._end_results_if_task_queue_closed()

    def requeue_task(self, task):
        """Requeues a task in the process."""

        with self._mutex:
            self._task_queue.push(task)

