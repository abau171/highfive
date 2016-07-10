"""Master for a distributed system."""

import highfive.distributed_process
import highfive.task_manager
import highfive.server


class LastResult(Exception):
    """Exception raised when a process user view runs out of results."""
    pass


class DistributedProcessUserView:
    """View of a distributed process object which a user can manipulate."""

    def __init__(self, process):
        self._process = process
        self._internal_results = process.results()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        """Closes the distributed process."""

        self._process.close()

    def results(self):
        """Iterates over the results of the distributed process."""

        return self._process.results()

    def next_result(self):
        """Returns one result at a time from the distributed process."""

        try:
            return next(self._internal_results)
        except StopIteration as e:
            raise LastResult from e


class Master:
    """Master object which handles the worker server and process queue.

    Starts a server listening for workers at a given host and port, and
    initializes a process queue from which the remote workers will execute
    tasks. The processes are run in the order they are created, but the tasks
    in them may be executed in any order (though generally they will execute
    nearly in order.

    """

    def __init__(self, host, port):
        self._task_mgr = highfive.task_manager.TaskManager()
        self._server = highfive.server.ServerThread(host, port, self._task_mgr)
        self._server.start()

    def process(self, task_iterable):
        p = highfive.distributed_process.DistributedProcess(task_iterable)
        self._task_mgr.add_process(p)
        return DistributedProcessUserView(p)

