# ----------------------------------------------------------------------------
# "THE BEER-WARE LICENSE" (Revision 42):
# <git@lerya.net> wrote this file. As long as you retain this notice you can
# do whatever you want with this stuff. If we meet some day, and you think
# this stuff is worth it, you can buy me a beer in return. Vincent Brillault
# ----------------------------------------------------------------------------

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

    # Those import DO exist, event if some IDE/pylint fail to understand it
    MASK = (pyinotify.IN_Q_OVERFLOW |  # pylint: disable=E1101
            pyinotify.IN_UNMOUNT |  # pylint: disable=E1101
            pyinotify.IN_IGNORED)  # pylint: disable=E1101

    def my_init(self, parent=None, watch_manager=None,  # pylint: disable=W0221
                watch_descriptors=None, wd_lock=None):
        """Called by pyinotify.ProcessEvent.__init__()
        pyinotify documentation explicitly warns against modifying __init__,
        thus we need to define our variables here, thus we violate W0201.
        Parent my_init uses **kargs, thus we violate W0221."""
        assert(parent is not None)
        assert(watch_manager is not None)
        assert(watch_descriptors is not None)
        assert(wd_lock is not None)
        self._parent = parent  # pylint: disable=W0201
        self._wm = watch_manager  # pylint: disable=W0201
        self._wd = watch_descriptors  # pylint: disable=W0201
        self._wd_lock = wd_lock  # pylint: disable=W0201

    def process_IN_UNMOUNT(self, event):  # pylint: disable=C0103
        """The whole fs was unmount, global failure !
        Non-standard method name set by libnotify"""
        self._parent.die_on(warn(InotifyFSUnmount(event)))

    def process_IN_Q_OVERFLOW(self, event):  # pylint: disable=C0103
        """The event queue overflowed, global failure !
        Non-standard method name set by libnotify"""
        self._parent.die_on(warn(InotifyQueueOverflow(event)))

    def process_IN_IGNORED(self, event):  # pylint: disable=C0103
        """This watch will now be ignored by pynotify
        Non-standard method name set by libnotify"""
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

    # Those import DO exist, event if some IDE/pylint fail to understand it
    MASK = (_DefaultEventProcessing.MASK |
            pyinotify.IN_CLOSE_WRITE |  # pylint: disable=E1101
            pyinotify.IN_MOVED_FROM |  # pylint: disable=E1101
            pyinotify.IN_MOVED_TO |  # pylint: disable=E1101
            pyinotify.IN_CREATE |  # pylint: disable=E1101
            pyinotify.IN_DELETE)  # pylint: disable=E1101

    def my_init(self, remove_scheduler=None, delay=10, outqueue=None,
                **kargs):
        """Called by pyinotify.ProcessEvent.__init__()
        pyinotify documentation explicitly warns against modifying __init__,
        thus we need to define our variables here, thus we violate W0201."""
        super(_EventProcessing, self).my_init(**kargs)
        assert(remove_scheduler is not None)
        assert(outqueue is not None)
        self._moving = {}  # pylint: disable=W0201
        self._move_lock = threading.RLock()  # pylint: disable=W0201
        self._scheduler = remove_scheduler  # pylint: disable=W0201
        self._delay = delay  # pylint: disable=W0201
        self._queue = outqueue  # pylint: disable=W0201

    def process_IN_CLOSE_WRITE(self, event):  # pylint: disable=C0103
        """A file was modified
        Non-standard method name set by libnotify"""
        self._queue.put(('modified', event.pathname))

    def process_IN_MOVED_FROM(self, event):  # pylint: disable=C0103
        """A file/folder was moved in
        Non-standard method name set by libnotify"""
        with self._move_lock:
            self._moving[event.cookie] = event.pathname
            self._scheduler.add(self._delay, event.cookie, self.delete,
                                (event.cookie, event.dir))

    def process_IN_MOVED_TO(self, event):  # pylint: disable=C0103
        """A file/folder was moved out
        Non-standard method name set by libnotify"""
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
                if event.dir:
                    self._add_rec(event)
                else:
                    self._queue.put(('modified', event.pathname))

    def process_IN_CREATE(self, event):  # pylint: disable=C0103
        """A file/folder was created
        Non-standard method name set by libnotify"""
        # If it's a file, 'modified' will kick-in, otherwise manually auto_add
        # (strange things can happen with mkdir -p a/b/c)
        if event.dir:
            self._add_rec(event)
            return

    def process_IN_DELETE(self, event):  # pylint: disable=C0103
        """A file/folder was deleted
        Non-standard method name set by libnotify"""
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

    def delete(self, data):
        """Callback from the scheduler"""
        (cookie, is_dir) = data
        with self._move_lock:
            path = None
            try:
                path = self._moving[cookie]
                del self._moving[cookie]
            except KeyError:
                # Already removed
                return
            if not is_dir:
                self._queue.put(('remove_file', path))
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
    # Those import DO exist, event if some IDE/pylint fail to understand it
    MASK = (_EventProcessing.MASK |
            pyinotify.IN_DELETE_SELF |  # pylint: disable=E1101
            pyinotify.IN_MOVE_SELF)  # pylint: disable=E1101

    def process_IN_DELETE_SELF(self, event):  # pylint: disable=C0103
        """The root folder was deleted, failure
        Non-standard method name set by libnotify"""
        self._parent.die_on(InotifyRootDeleted(event))

    def process_IN_MOVE_SELF(self, event):  # pylint: disable=C0103
        """The root folder was moved, failure
        Non-standard method name set by libnotify"""
        # TODO: support move inside watched dirs
        self._parent.die_on(InotifyRootMoved(event))


class InotifyWatch(object):
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
        ev_proc = _EventProcessing(**kargs)
        self._rootevent_process = _RootEventProcessing(**kargs)

        self._notifier = pyinotify.ThreadedNotifier(self._wm,
                                                    default_proc_fun=ev_proc)

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

    def started(self):
        """"Check if we started"""
        return self._started

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
