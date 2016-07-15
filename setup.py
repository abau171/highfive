from setuptools import setup

long_desc = """
HighFive is a simple distributed processing framework designed to support a large pool of transient workers coming and going as they please. Workers running in the pool are invisible to the owner, so there is no need to worry about distributing tasks manually or the possibility of network issues as these are handled by the framework. HighFive is made for cooperative processing of CPU-heavy tasks by allowing you and your friends to chip in as much computing power as you want, for as long as you want!

HighFive is written in Python 3, and uses asyncio for managing connections. Because it uses async/await syntax, it is currently only supported by Python 3.5 and up (this may change in the future).

"""

setup(
    name = "highfive",
    packages = ["highfive"],
    version = "0.1",
    description =
        "Framework for fault-tolerant cooperative distributed processing.",
    long_description = long_desc,
    author = "Andrew Bauer",
    author_email = "abau171@gmail.com",
    url = "https://github.com/abau171/highfive",
    download_url = "https://github.com/abau171/highfive/archive/v0.1.tar.gz",
    license = "MIT",
    keywords =
        ["distributed", "queue", "task queue", "job queue", "collaborative"],
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Internet",
        "Topic :: Software Development",
        "Topic :: System :: Distributed Computing",
    ],
)

