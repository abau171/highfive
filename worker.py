import socket
import multiprocessing


class WorkerProcess(multiprocessing.Process):

    def __init__(self, hostname, port):
        super().__init__()
        self.hostname = hostname
        self.port = port

    def main(self):
        print("k")

    def run(self):
        with socket.socket() as client_socket:
            client_socket.connect((self.hostname, self.port))
            self.main()


def run_workers(hostname, port, num_workers):
    workers = []
    if num_workers == 0:
        num_workers = multiprocessing.cpu_count()
    for _ in range(num_workers):    
        worker = WorkerProcess(hostname, port)
        worker.start()
        workers.append(worker)
    for worker in workers:
        worker.join()

