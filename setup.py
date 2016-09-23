from setuptools import setup

long_desc = """
HighFive is a simple distributed processing framework designed so you and your friends can donate CPU time to each other to run large, interesting computations. It's easy to spin up a HighFive master and connect as many remote workers as you want to it!

Don't want to lock down all your computers for hours while you wait for results? Since HighFive was designed for cooperative work, the workers can
disconnect at any time without harming the main process! Then, they can reconnect to the same process later and get back to work!

HighFive is written in Python 3, and uses asyncio for managing connections. Because it uses async/await syntax, it is currently only supported by Python 3.5 and up.
"""

setup(
    name = "highfive",
    packages = ["highfive"],
    version = "0.2",
    description =
        "Framework for fault-tolerant cooperative distributed processing.",
    long_description = long_desc,
    author = "Andrew Bauer",
    author_email = "abau171@gmail.com",
    url = "https://github.com/abau171/highfive",
    download_url = "https://github.com/abau171/highfive/archive/v0.2.tar.gz",
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
    test_suite = "test",
)

