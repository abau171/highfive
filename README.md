HighFive
========
Framework for fault-tolerant cooperative distributed processing.

### About

HighFive is a simple distributed processing framework designed to support a
large pool of transient workers coming and going as they please. Workers
running in the pool are invisible to the owner, so there is no need to worry
about distributing tasks manually or the possibility of network issues as
these are handled by the framework. HighFive is made for cooperative
processing of CPU-heavy tasks by allowing you and your friends to chip in as
much computing power as you want, for as long as you want!

HighFive is written in Python 3, and uses
[asyncio](https://docs.python.org/3/library/asyncio.html) for managing
connections. Because it uses
[`async/await`](https://www.python.org/dev/peps/pep-0492/) syntax, it is
currently only supported by Python 3.5 and up (this may change in the future).

### Installation

HighFive will soon be available on [PyPI](https://pypi.python.org/pypi).

Until then, you can install HighFive manually:

1. Clone the repository.
2. `cd` into the root directory.
3. (optional) Create a
[virtual environment](http://docs.python-guide.org/en/latest/dev/virtualenvs/).
4. Run `pip install .`.

### Example

You can see a simple example of HighFive in the `example/` directory of this
project, and run it for yourself! After installation, simply run the master
with `python3 example/add_master.py`, then join the worker pool with `python3
example/add_worker.py`.

More thorough documentation is coming soon!

