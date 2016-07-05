import task_manager
import server


class Master:

    def __init__(self, host, port):
        self.task_mgr = task_manager.TaskManager()
        self.server = server.ServerThread(host, port, self.task_mgr)
        self.server.start()

    def process(self, task_iterable):
        return self.task_mgr.process(task_iterable)

