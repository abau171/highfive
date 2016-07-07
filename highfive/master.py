import highfive.distributed_process
import highfive.task_manager
import highfive.server


class DistributedProcessUserView:

    def __init__(self, process):
        self._process = process

    def close(self):
        self._process.close()

    def results(self):
        return self._process.results()


class Master:

    def __init__(self, host, port):
        self._task_mgr = highfive.task_manager.TaskManager()
        self._server = highfive.server.ServerThread(host, port, self._task_mgr)
        self._server.start()

    def process(self, task_iterable):
        p = highfive.distributed_process.DistributedProcess(task_iterable)
        self._task_mgr.add_process(p)
        return DistributedProcessUserView(p)

