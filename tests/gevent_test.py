"""
adapted from https://sdiehl.github.io/gevent-tutorial/
"""

import gevent
from gevent import Greenlet
from stackcollector import stacksampler

gevent.spawn(stacksampler.run_profiler)


class MyGreenlet(Greenlet):
    def __init__(self, message, n):
        Greenlet.__init__(self)
        self.message = message
        self.n = n

    def _run(self):
        print(self.message)
        gevent.sleep(self.n)


def main():
    print("Use Ctrl+C to stop")
    g = MyGreenlet("Hi there!", 3)
    g.start()
    g.join()


if __name__ == '__main__':
    main()
