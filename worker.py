import socket
import multiprocessing


class WorkerProcess(multiprocessing.Process):

    def __init__(self, hostname, port):
        super().__init__()
        self.hostname = hostname
        self.port = port

    def main(self):
        read_file = self.client_socket.makefile("r")
        while True:
            print(read_file.readline().rstrip("\n"))

    def run(self):
        with socket.socket() as self.client_socket:
            self.client_socket.connect((self.hostname, self.port))
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

