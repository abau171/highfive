"""Manages the queue of processes and the execution of tasks from them."""

import threading
import collections
import contextlib

import highfive.distributed_process


# Used instead of 'None' to represent no results from a task.
NO_RESULT = object()


class Task:
    """Base class for tasks, which store the state of their execution.

    Users should subclass this task, then override the run method which should
    use the provided connection to execute the task. Inside the run method,
    users may call 'done' to indicate that the task has been completed and
    optionally provide a result from the task.

    """

    def __init__(self):
        self._done = False
        self._result = NO_RESULT

    def done(self, result=NO_RESULT):
        """Indicates that the task has been completed.

        If a result has been found during execution, it should be given as the
        'result' parameter. Note that 'None' is a valid result which is
        different from not providing a result.

        """

        self._result = result
        self._done = True

    def is_done(self):
        """Determines whether the task has been completed."""

        return self._done

    def run(self, connection):
        """Implemented by users to execute a task using a worker connection."""

        raise NotImplementedError

    def get_result(self):
        """Gets the result of the task after it has been executed."""

        return self._result


class TaskManager:
    """Manages both the process queue and the execution of tasks.

    Processes are added to the process queue, and have their tasks executed
    by remote workers until the process is complete. Processes are run in the
    order they are provided, however their tasks may be executed out-of-order.

    Both adding processes and receiving a task to execute are thread-safe.

    """

    def __init__(self):

        self._mutex = threading.Lock()
        self._queue_update = threading.Condition(self._mutex)

        self._process_queue = collections.deque()

    def add_process(self, process):
        """Adds a process to the process queue."""

        with self._queue_update:
            self._process_queue.append(process)
            self._queue_update.notify()

    @contextlib.contextmanager
    def task(self):
        """Recieves a task from the process currently being executed.

        The task may then be executed and its result will be reported or,
        in the case where the task was not executed properly, added back to
        the process to be run again later.

        This method may block until a task becomes available to be executed.

        """

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
            # No need to lock here, not using the queue and the process locks
            # itself.
            # TODO Move task report result/requeue logic to DistributedProcess?
            if task.is_done():
                process.task_done(task.get_result())
            else:
                process.requeue_task(task)

