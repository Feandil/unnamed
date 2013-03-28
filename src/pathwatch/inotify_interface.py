'''Interface with pyinotify'''

import os.path
import pyinotify
import threading
from warnings import warn

from .errorwarn import (InotifyFSUnmount, InotifyQueueOverflow,
                        InotifyUnexpectedEvent, InotifyTranscientPath,
                        InotifyDisappearingWD, InotifyMissingingWD,
                        InotifyRootDeleted, InotifyRootMoved)
from .remove_scheduler import RemoveScheduler


class _DefaultEventProcessing(pyinotify.ProcessEvent):
    """Base event processing, cover the weird cases"""

    # Those import DO exist, event if some IDE fail to understand it
    MASK = (pyinotify.IN_UNMOUNT | pyinotify.IN_Q_OVERFLOW |  # IGNORE:E1101
            pyinotify.IN_IGNORED)  # IGNORE:E1101

    def my_init(self, parent=None, watch_manager=None,  # IGNORE:W0221
                watch_descriptors=None, wd_lock=None):
        """Called by pyinotify.ProcessEvent.__init__()
        pyinotify documentation explicitly warns against modifying __init__,
        thus we need to define our variables here, thus IGNORE:W0201"""
        assert(parent is not None)
        assert(watch_manager is not None)
        assert(watch_descriptors is not None)
        assert(wd_lock is not None)
        self._parent = parent  # IGNORE:W0201
        self._wm = watch_manager  # IGNORE:W0201
        self._wd = watch_descriptors  # IGNORE:W0201
        self._wd_lock = wd_lock  # IGNORE:W0201

    def process_IN_UNMOUNT(self, event):  # IGNORE:C0103
        """The whole fs was unmount, global failure !"""
        self._parent.die_on(warn(InotifyFSUnmount(event)))

    def process_IN_Q_OVERFLOW(self, event):  # IGNORE:C0103
        """The event queue overflowed, global failure !"""
        self._parent.die_on(warn(InotifyQueueOverflow(event)))

    def process_IN_IGNORED(self, event):  # IGNORE:C0103
        """This watch will now be ignored by pynotify"""
        try:
            with self._wd_lock:
                del self._wd[event.pathname]
        except KeyError:
            warn(InotifyMissingingWD(event))

    def process_default(self, event):
        """An event was not processed by other systems, failure !"""
        warn(InotifyUnexpectedEvent(event))


class _EventProcessing(_DefaultEventProcessing):
    """Process events from inotify on normal nodes"""

    # Those import DO exist, event if some IDE fail to understand it
    MASK = (_DefaultEventProcessing.MASK | pyinotify.IN_CLOSE_WRITE |  # IGNORE:E1101
            pyinotify.IN_MOVED_FROM | pyinotify.IN_MOVED_TO |  # IGNORE:E1101
            pyinotify.IN_CREATE | pyinotify.IN_DELETE)  # IGNORE:E1101

    def my_init(self, remove_scheduler=None, delay=10, outqueue=None,
                **kargs):
        """Called by pyinotify.ProcessEvent.__init__()
        pyinotify documentation explicitly warns against modifying __init__,
        thus we need to define our variables here, thus IGNORE:W0201"""
        super(_EventProcessing, self).my_init(**kargs)
        assert(remove_scheduler is not None)
        assert(outqueue is not None)
        self._moving = {}  # IGNORE:W0201
        self._move_lock = threading.RLock()  # IGNORE:W0201
        self._scheduler = remove_scheduler  # IGNORE:W0201
        self._delay = delay  # IGNORE:W0201
        self._queue = outqueue  # IGNORE:W0201

    def process_IN_CLOSE_WRITE(self, event):  # IGNORE:C0103
        """A file was modified"""
        self._queue.put(('modified', event.pathname))

    def process_IN_MOVED_FROM(self, event):  # IGNORE:C0103
        """A file/folder was moved in"""
        with self._move_lock:
            self._moving[event.cookie] = event.pathname
            self._scheduler.add(self._delay, event.cookie, self)

    def process_IN_MOVED_TO(self, event):  # IGNORE:C0103
        """A file/folder was moved out"""
        self._scheduler.cancel(event.cookie)
        with self._move_lock:
            if event.cookie in self._moving:
                # moved from watched place
                assert(self._moving[event.cookie] == event.src_pathname)
                del self._moving[event.cookie]
                if event.dir:
                    self._queue.put(('move_dir', event.src_pathname,
                                     event.pathname))
                else:
                    self._queue.put(('move_file', event.src_pathname,
                                     event.pathname))

            else:
                # Moved from an external place: new :)
                assert(not hasattr(event, 'src_pathname'))
                self._add_rec(event)

    def process_IN_CREATE(self, event):  # IGNORE:C0103
        """A file/folder was created"""
        # If it's a file, 'modified' will kick-in, otherwise manually auto_add
        # (strange things can happen with mkdir -p a/b/c)
        if event.dir:
            self._add_rec(event)
            return

    def process_IN_DELETE(self, event):  # IGNORE:C0103
        """A file/folder was deleted"""
        # The watcher is removed later by IN_IGNORE
        if event.dir:
            self._queue.put(('remove_dir', event.pathname))
        else:
            self._queue.put(('remove_file', event.pathname))

    def _add_rec(self, event):
        """Recursiverly add a folder to the watched ones"""
        try:
            with self._wd_lock:
                self._wd.update(self._wm.add_watch(event.pathname,
                                                   _EventProcessing.MASK,
                                                   rec=True,
                                                   auto_add=False,
                                                   quiet=False))
        except pyinotify.WatchManagerError as err:
            # The path disappeared
            warn(InotifyTranscientPath("{}:{}".format(event.pathname, err)))
            return

        # The listener should use the scanner to find sub-files/folders
        self._queue.put(('new_dir', event.pathname))

    def delete(self, item):
        """Callback from the scheduler"""
        with self._move_lock:
            path = None
            try:
                path = self._moving[item]
                del self._moving[item]
            except KeyError:
                # Already removed
                return
            with self._wd_lock:
                remove_wd = []
                for k in self._wd.keys():
                    if k.startswith(path):
                        remove_wd.append(self._wd[k])
                if len(remove_wd) != 0:
                    try:
                        self._wm.rm_watch(remove_wd, rec=False, quiet=False)
                    except pyinotify.WatchManagerError as err:
                        warn(InotifyDisappearingWD(err))
                        for key in remove_wd:
                            try:
                                self._wm.rm_watch(key, rec=False, quiet=False)
                            except pyinotify.WatchManagerError:
                                pass
            self._queue.put(('remove_dir', path))


class _RootEventProcessing(_EventProcessing):
    """Process events from inotify"""
    # Those import DO exist, event if some IDE fail to understand it
    MASK = (_EventProcessing.MASK | pyinotify.IN_DELETE_SELF |  # IGNORE:E1101
            pyinotify.IN_MOVE_SELF)  # IGNORE:E1101

    def process_IN_DELETE_SELF(self, event):  # IGNORE:C0103
        """The root folder was deleted, failure"""
        self._parent.die_on(InotifyRootDeleted(event))

    def process_IN_MOVE_SELF(self, event):  # IGNORE:C0103
        """The root folder was moved, failure"""
        # TODO: support move inside watched dirs IGNORE:W0511
        self._parent.die_on(InotifyRootMoved(event))


class InotifyWatch(object):  # IGNORE:R0902
    """Interface to inotify, watch any folder"""

    def __init__(self, listener, delay=2):
        self._wm = pyinotify.WatchManager()
        self._wd = {}
        self._wd_lock = threading.RLock()
        self._scheduler = RemoveScheduler()
        self._queue = listener
        kargs = {'parent': self,
                 'watch_manager': self._wm,
                 'watch_descriptors': self._wd,
                 'wd_lock': self._wd_lock,
                 'remove_scheduler': self._scheduler,
                 'delay': delay,
                 'outqueue': self._queue}
        event_process = _EventProcessing(**kargs)
        self._rootevent_process = _RootEventProcessing(**kargs)

        self._notifier = pyinotify.ThreadedNotifier(self._wm,
                                                    default_proc_fun=event_process)  # IGNORE:C0301

        self._started = False

    def start(self):
        """Start the threads, must not be called twice on the same instance"""
        assert(not self._started)
        self._scheduler.start()
        self._notifier.start()
        self._started = True

    def stop(self):
        """"Stop the threads, nop if non started"""
        if self._started:
            self._scheduler.stop()
            self._notifier.stop()
        self._started = False

    def add(self, root):
        """Add a folder to the list of watched folders"""
        assert(os.path.isdir(root))
        with self._wd_lock:
            self._wd.update(self._wm.add_watch(root,
                                               _EventProcessing.MASK,
                                               rec=True,
                                               auto_add=False,
                                               quiet=False))
            try:
                new_wd = self._wd[root]
                self._wm.update_watch(new_wd, mask=_RootEventProcessing.MASK,
                                      proc_fun=self._rootevent_process,
                                      quiet=False)
            except KeyError:
                return False
        return True

    def die_on(self, warning):
        """Dying callback"""
        self._queue.put(('DIE', warning))
        self.stop()
