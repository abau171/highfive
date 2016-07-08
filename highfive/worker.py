"""Remote workers which connect to a master server to run tasks.

Workers should be subclasses by users to be able to recieve data from a task
and execute the heavy-duty processing.

Multiple workers can be created at a time to connect to a single master server.

"""

import socket
import multiprocessing

import highfive.message_connection


class Worker:
    """A base worker class to be subclasses by users."""

    def run(self, connection):
        """Implement this to be able to handle processing of data."""

        raise NotImplementedError


class WorkerProcess(multiprocessing.Process):
    """A single remote worker which connects to the central master server."""

    def __init__(self, hostname, port, worker):
        super().__init__()
        self._hostname = hostname
        self._port = port
        self._worker = worker

    def handle_work(self):
        """Opens a connection to the master server and runs the worker."""

        with socket.socket() as client_socket:
            try:
                client_socket.connect((self._hostname, self._port))
            except ConnectionRefusedError:
                print("could not connect to server.")
            connection = highfive.message_connection.MessageConnection(
                client_socket)
            try:
                self._worker.run(connection)
            except highfive.message_connection.Closed:
                pass
        print("connection closed.")

    def run(self):
        """Catches keyboard interrupts to allow for clean exits."""

        try:
            self.handle_work()
        except KeyboardInterrupt:
            pass


def run_workers(hostname, port, worker, count=0):
    """Starts a number of workers, then waits for them to exit."""

    if count == 0:
        count = multiprocessing.cpu_count()

    worker_procs = []
    for _ in range(count):
        worker_proc = WorkerProcess(hostname, port, worker)
        worker_proc.start()
        worker_procs.append(worker_proc)

    for worker_proc in worker_procs:
        worker_proc.join()

