"""Extend sched with threading, with automatic run/wait"""

import sched
import time
import threading

# Note: as of Python3.3, the lock can be removed

class RemoveScheduler(threading.Thread):
    """Run a scheduler in a dedicated thread. Wait on an 'add' if schedule empty
        Warning: no concurrency protection between last event and add !
        
        This scheduler is specialized into removing entries from a dictionary.
    """

    def __init__(self, target, timefunc=time.time, delayfunc=time.sleep):
        self._target = target
        self._elements = {}
        self._sched = sched.scheduler(timefunc, delayfunc)
        self._lock = threading.RLock()
        self._wakeup = threading.Condition(self._lock)
        self._end = threading.Event()
        super(RemoveScheduler, self).__init__()

    def _remove_element(self, item):
        """Callback for the scheduler"""
        try:
            del self._elements[item]
            del self._target[item]
        except KeyError:
            # Already removed
            pass

    def add(self, delay, item, priority=1):
        """Remove the given item in delay"""
        with self._lock:
            if item in self._elements:
                raise KeyError("Key already present")
            event = self._sched.enter(delay, priority,
                                      self._remove_element,
                                      (item,))
            self._elements[item] = event
            self._wakeup.notify_all()

    def cancel(self, item):
        """Cancel the removal of the given item"""
        with self._lock:
            try:
                event = self._elements[item]
                del self._elements[item]
                self._sched.cancel(event)
            except KeyError:
                # Already removed
                pass

    def run(self):
        """Run the scheduler, rerun-it until the end"""
        while not self._end.is_set():
            self._sched.run()
            with self._lock:
                if self._end.is_set():
                    break
                self._wakeup.wait()

    def stop(self):
        """Notify the underlying thread to stop"""
        self._end.set()
        with self._lock:
            self._wakeup.notify_all()
