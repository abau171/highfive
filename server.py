import socket
import threading
import contextlib


class WorkerRegistrar:

    def __init__(self):
        self.worker_conns = set()
        self.mutex = threading.Lock()

    @contextlib.contextmanager
    def registered(self, worker_conn):
        with self.mutex:
            self.worker_conns.add(worker_conn)
        try:
            yield
        finally:
            with self.mutex:
                self.worker_conns.remove(worker_conn)


class WorkerConnectionThread(threading.Thread):

    def __init__(self, client_socket, registrar, task_mgr):
        super().__init__(daemon=True)
        self.client_socket = client_socket
        self.registrar = registrar
        self.task_mgr = task_mgr

    def handle_tasks(self):
        try:
            while True:
                with self.task_mgr.task() as task:
                    task.run(self.client_socket)
        except OSError:
            pass

    def run(self):
        with self.registrar.registered(self):
            self.handle_tasks()


class ServerThread(threading.Thread):

    def __init__(self, hostname, port, task_mgr):
        super().__init__(daemon=True)
        self.hostname = hostname
        self.port = port
        self.registrar = WorkerRegistrar()
        self.task_mgr = task_mgr

    def run(self):
        with socket.socket() as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((self.hostname, self.port))
            server_socket.listen(5)
            while True:
                client_socket, address = server_socket.accept()
                WorkerConnectionThread(client_socket, self.registrar, self.task_mgr).start()

