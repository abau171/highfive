import socket
import threading
import contextlib


class WorkerRegistrar:

    def __init__(self):
        self._worker_conns = set()
        self._mutex = threading.Lock()

    @contextlib.contextmanager
    def registered(self, worker_conn):
        with self._mutex:
            self._worker_conns.add(worker_conn)
        try:
            yield
        finally:
            with self._mutex:
                self._worker_conns.remove(worker_conn)


class WorkerConnectionThread(threading.Thread):

    def __init__(self, client_socket, registrar, task_mgr):
        super().__init__(daemon=True)
        self._client_socket = client_socket
        self._registrar = registrar
        self._task_mgr = task_mgr

    def _handle_tasks(self):
        try:
            while True:
                with self._task_mgr.task() as task:
                    task.run(self._client_socket)
        except OSError:
            pass

    def run(self):
        with self._registrar.registered(self):
            self._handle_tasks()


class ServerThread(threading.Thread):

    def __init__(self, hostname, port, task_mgr):
        super().__init__(daemon=True)
        self._hostname = hostname
        self._port = port
        self._registrar = WorkerRegistrar()
        self._task_mgr = task_mgr

    def run(self):
        with socket.socket() as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((self._hostname, self._port))
            server_socket.listen(5)
            while True:
                client_socket, address = server_socket.accept()
                WorkerConnectionThread(client_socket, self._registrar, self._task_mgr).start()

