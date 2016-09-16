import asyncio

import highfive


class AddJob(highfive.Job):

    def __init__(self, a, b):

        self._a = a
        self._b = b

    def get_call(self):

        return [self._a, self._b]

    def get_result(self, response):

        return (self._a, self._b, response)


def main():

    with highfive.MasterDaemon() as master:

        jobs = (AddJob(i, i * i) for i in range(100, 200))
        with master.run(jobs) as js:
            for a, b, c in js.results():
                print("{} + {} = {}".format(a, b, c))
                if a == 150:
                    break

        jobs = ([i, i * i] for i in range(100))
        js = master.run(jobs)
        for c in js.results():
            print(c)


if __name__ == "__main__":

    main()

