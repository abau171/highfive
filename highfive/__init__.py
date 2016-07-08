"""HighFive distributed processing framework.

A HighFive master can be created which accepts connections from several
remote workers. Then, new distributed processes can be run using the master
which distributes the tasks of the process to its connected workers.

Worker disconnects are handled cleanly and invisibly to the master process
without affecting the execution of the distributed process being executed.

"""

from highfive.master import Master
from highfive.task_manager import Task
from highfive.worker import run_workers, Worker

