import socket
import multiprocessing


class Worker:

    def run(self, connection):
        raise NotImplementedError


class WorkerProcess(multiprocessing.Process):

    def __init__(self, hostname, port, worker):
        super().__init__()
        self.hostname = hostname
        self.port = port
        self.worker = worker

    def run(self):
        with socket.socket() as client_socket:
            client_socket.connect((self.hostname, self.port))
            self.worker.run(client_socket)


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

