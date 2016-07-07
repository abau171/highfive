import socket
import multiprocessing


class Worker:

    def run(self, connection):
        raise NotImplementedError


class WorkerProcess(multiprocessing.Process):

    def __init__(self, hostname, port, worker):
        super().__init__()
        self._hostname = hostname
        self._port = port
        self._worker = worker

    def handle_work(self):
        with socket.socket() as client_socket:
            try:
                client_socket.connect((self._hostname, self._port))
                self._worker.run(client_socket)
            except ConnectionRefusedError:
                print("could not connect to server.")
            except OSError:
                pass

    def run(self):
        try:
            self.handle_work()
        except KeyboardInterrupt:
            pass


def run_workers(hostname, port, worker, count=0):

    if count == 0:
        count = multiprocessing.cpu_count()

    worker_procs = []
    for _ in range(count):
        worker_proc = WorkerProcess(hostname, port, worker)
        worker_proc.start()
        worker_procs.append(worker_proc)

    for worker_proc in worker_procs:
        worker_proc.join()

