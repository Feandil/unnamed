'''Central class, include every other class'''

import threading
import Queue

from .database import DBRootHelper, DBFilesHelper
from .hasher import Hasher
from .inotify_interface import InotifyWatch
from .scanner import Scanner


def _is_in(path, roots):
    """Check if a path is in a collection of roots"""
    for root in roots:
        if path.startswith(root):
            return True
    return False


class PathWatch(threading.Thread):
    """Watch all the given roots store tree with hashes in database"""

    def __init__(self, database, inotify_delay=2, files_per_call=10):
        super(PathWatch, self).__init__()
        self._database = database
        self._filedb = None
        self._inc_queue = Queue.Queue()
        self._inotify = InotifyWatch(self._inc_queue, inotify_delay)
        self._scanner = None
        self._hasher = Hasher(database, files_per_call)
        self._end = threading.Event()
        self._lock = threading.Lock()

    def _apply_update(self, update, notify=True, concurrent=None):
        """Apply an inotify update"""
        cmd = update[0]
        if cmd == 'DIE':
            self.die(update[1])
        elif cmd == 'modified':
            self._scanner.scan_file(update[1])
            if notify:
                self._hasher.notify()
        elif cmd == 'move_dir':
            self._filedb.move_dir(update[1], update[2])
        elif cmd == 'move_file':
            self._filedb.move_file(update[1], update[2])
        elif cmd == 'remove_dir':
            self._filedb.delete_path(update[1])
        elif cmd == 'remove_file':
            self._filedb.delete_single(update[1])
        elif cmd == 'new_dir':
            if concurrent is None:
                self._check_path(update[1], notify)
            else:
                concurrent.add(update[1])
        else:
            self.die('Unknown innotify_update: {}'.format(';'.join(update)))

    def _check_path(self, path, notify=True):
        """Re-scan one path and recursively scan concurrent modifications"""
        self._check_paths([path], notify)

    def _check_paths(self, paths, notify=True):
        """Re-scan those paths and recursively scan concurrent modifications"""
        for path in paths:
            self._scanner.scan(path)
        concurrent_updates = set()
        while not self._inc_queue.empty():
            message = self._inc_queue.get()
            if (_is_in(message[1], paths) or
                    (len(message) > 2 and _is_in(message[2], paths))):
                for path in message[1:]:
                    concurrent_updates.add(path)
            else:
                self._apply_update(message, notify, concurrent_updates)
        if len(concurrent_updates) != 0:
            self._check_paths(concurrent_updates, notify)
        elif notify:
            self._hasher.notify()

    def run(self):
        """Called by start, should not be runned direclty"""
        self._filedb = DBFilesHelper(self._database)
        self._scanner = Scanner(self._database)
        with self._lock:
            self._inotify.start()
            db_root = DBRootHelper(self._database)
            root_list = db_root.list_roots()
            db_root.close()
            for root in root_list:
                self._inotify.add(root)
            self._check_paths(root_list, notify=False)
            self._hasher.start()
        while not self._end.is_set():
            update = self._inc_queue.get()
            if self._end.is_set():
                break
            with self._lock:
                self._apply_update(update)
        self._inotify.stop()
        self._hasher.stop()
        self._scanner.close()

    def die(self, reason):
        """Die for some given reason"""
        print 'Error, dying:'
        print reason
        self.stop()

    def stop(self):
        """Stop everything"""
        self._end.set()
        self._inc_queue.put(('Shutdown'))
        self.join()

    def add_root(self, path):
        """Add a root to the database"""
        db_root = DBRootHelper(self._database)
        scanner = Scanner(self._database)
        with self._lock:
            db_root.add_root(path)
            if self._inotify.started():
                self._inotify.add(path)
            scanner.scan(path)
        scanner.close()
        db_root.close()
