"""Server which accepts connections from remote workers.

When a remote worker connects, it begins processing tasks it registers itself,
then begins receiving work from the task manager.

"""

import socket
import threading
import contextlib

import highfive.message_connection


class WorkerRegistry:
    """Tracks all active workers.
    
    Registration and deregistration are thread-safe.
    
    """

    def __init__(self):
        self._worker_conns = set()
        self._mutex = threading.Lock()

    @contextlib.contextmanager
    def registered(self, worker_conn):
        """Manages registration and deregistration of worker connections."""

        with self._mutex:
            self._worker_conns.add(worker_conn)
        try:
            yield
        finally:
            with self._mutex:
                self._worker_conns.remove(worker_conn)


class WorkerConnectionThread(threading.Thread):
    """A connection to a single remote worker which can execute tasks.

    Connected workers register themselves, then begin executing tasks received
    from the task manager. Exceptions recieved from the task being executed
    both cause the task to fail and close the connection to the worker. The
    connection is closed because there is no guarantee that any exception
    raised by a task will leave the remote worker in a consistent state.

    """

    def __init__(self, client_socket, registry, task_mgr):
        super().__init__(daemon=True)
        self._client_socket = client_socket
        self._connection = highfive.message_connection.MessageConnection(
            self._client_socket)
        self._registry = registry
        self._task_mgr = task_mgr

    def _handle_tasks(self):
        """Repeatedly gets and executes tasks from the task manager."""

        try:
            while not self._connection.is_closed():
                with self._task_mgr.task() as task:
                    task.run(self._connection)
        except highfive.message_connection.Closed:
            pass
        finally:
            self._client_socket.close()
            print("connection closed.")

    def run(self):
        """Registers the connection, then begins running tasks."""

        with self._registry.registered(self):
            self._handle_tasks()


class ServerThread(threading.Thread):
    """The server which accepts new connections.

    New worker connections are given the active task manager from which they
    get tasks to process.

    """

    def __init__(self, hostname, port, task_mgr):
        super().__init__(daemon=True)
        self._hostname = hostname
        self._port = port
        self._registry = WorkerRegistry()
        self._task_mgr = task_mgr

    def run(self):
        with socket.socket() as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((self._hostname, self._port))
            server_socket.listen(5)
            while True:
                client_socket, address = server_socket.accept()
                WorkerConnectionThread(
                    client_socket,
                    self._registry,
                    self._task_mgr).start()

