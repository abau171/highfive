HighFive
========
Framework for fault-tolerant cooperative distributed processing.

### About

HighFive is a simple distributed processing framework designed so you and your
friends can donate CPU time to each other to run large, interesting
computations. It's easy to spin up a HighFive master and connect as many remote
workers as you want to it!

Don't want to lock down all your computers for hours while you wait for
results? Since HighFive was designed for cooperative work, the workers can
disconnect at any time without harming the main process! Then, they can
reconnect to the same process later and get back to work!

HighFive is written in Python 3, and uses
[asyncio](https://docs.python.org/3/library/asyncio.html) for managing
connections. Because it uses
[`async/await`](https://www.python.org/dev/peps/pep-0492/) syntax, it is
currently only supported by Python 3.5 and up.

### Installation

HighFive is available on [PyPI](https://testpypi.python.org/pypi/highfive)!

Simply run `pip install highfive` to install the latest version.

You can also install HighFive manually:

1. clone the repository
2. `cd` into the root directory
3. (optional) create a
[virtual environment](http://docs.python-guide.org/en/latest/dev/virtualenvs/)
4. run `pip install .`

### Example

You can see a simple example of HighFive in the `example/` directory of this
project, and run it for yourself! After installation, simply run the master
with `python3 example/sum_master.py`, then join the worker pool with `python3
example/sum_worker.py`.

More thorough documentation is coming soon!

