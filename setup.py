from setuptools import setup

long_desc = """
HighFive is a distributed processing framework designed to be used for
collaborative computation. It is made to be fault-tolerant to network issues
and manual disconnects so workers can come and go as they please without
disrupting running processes.

"""

setup(
    name = "highfive",
    packages = ["highfive"],
    version = "0.0",
    description =
        "Fault-tolerant, cooperative distributed processing framework.",
    long_description = long_desc,
    author = "Andrew Bauer",
    author_email = "abau171@gmail.com",
    url = "https://github.com/abau171/highfive",
    #download_url = "",
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

