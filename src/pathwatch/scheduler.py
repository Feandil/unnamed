# ----------------------------------------------------------------------------
# "THE BEER-WARE LICENSE" (Revision 42):
# <git@lerya.net> wrote this file. As long as you retain this notice you can
# do whatever you want with this stuff. If we meet some day, and you think
# this stuff is worth it, you can buy me a beer in return. Vincent Brillault
# ----------------------------------------------------------------------------

"""Thread implementation of sched for Linux, with a proper poll"""

from fcntl import fcntl, F_SETFL
from heapq import heapify, heappop, heappush
from os import pipe, read, write, O_NONBLOCK
from select import poll, POLLIN
from time import time
from threading import Event, RLock, Thread

READ_MAX_SIZE = 8


class Scheduler(Thread):
    """Thread implementation of sched, with a proper poll"""

    def __init__(self):
        super(Scheduler, self).__init__()
        self._heap = []
        self._heap_content = {}
        self._lock = RLock()
        self._fd = pipe()
        self._pollobj = poll()
        self._pollobj.register(self._fd[0], POLLIN)
        fcntl(self._fd[0], F_SETFL, O_NONBLOCK)
        self._end = Event()
        self._next = None

    def add(self, delay, ident, callback, *args, **kwargs):
        """Run the given callback after the given delay,
        the ident must be unique and is used for cancelling the event"""
        end = time() + delay
        if ident in self._heap_content:
            raise KeyError('Identifier already present')
        self._heap_content[ident] = (end, callback, args, kwargs)
        with self._lock:
            heappush(self._heap, (end, ident))
            if (self._next is None or
                    self._next > end):
                write(self._fd[1], '.')

    def cancel(self, ident):
        """Cancel the removal of the callback identified by ident"""
        with self._lock:
            try:
                del self._heap_content[ident]
            except KeyError:
                # Already removed
                pass

    def _clean_fd(self):
        """Clean the pipe ((re-)enter the lock)"""
        with self._lock:
            try:
                while read(self._fd[0], READ_MAX_SIZE) != '':
                    pass
            except OSError:
                # EAGAIN (empty)
                pass

    def run(self):
        """Run the scheduler, rerun-it until the end"""
        while not self._end.is_set():
            # As we are only removing element in this function, no lock needed
            if len(self._heap) == 0:
                self._pollobj.poll()
                self._clean_fd()
                continue
            else:
                with self._lock:
                    (end, ident) = self._heap[0]
                    try:
                        raw = self._heap_content[ident]
                    except KeyError:
                        # Event Cancelled
                        heappop(self._heap)
                        continue
                    if raw[0] != end:
                        # Event Cancelled and re-injected
                        heappop(self._heap)
                        continue

            now = time()
            if now < end:
                res = self._pollobj.poll(end - now)
                if res is not None:
                    self._clean_fd()
                    continue
            with self._lock:
                self._heap.remove((end, ident))
                heapify(self._heap)
                try:
                    (end2, callback, args, kwargs) = self._heap_content[ident]
                    if end2 != end:
                        # Event Cancelled and re-injected
                        heappop(self._heap)
                        continue
                    del self._heap_content[ident]
                except KeyError:
                    continue
            callback(*args, **kwargs)

    def stop(self):
        """Notify the underlying thread to stop, join it"""
        self._end.set()
        with self._lock:
            write(self._fd[1], '!')
        self.join()
