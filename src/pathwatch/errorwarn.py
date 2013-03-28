'''Different errors/warnings issued by this module'''


class InotifyFSUnmount(Warning):
    """The underlying file-system was unmounted, stopping inotify daemon"""


class InotifyQueueOverflow(Warning):
    """The underlying queue overflowed, stopping inotify daemon"""


class InotifyUnexpectedEvent(Warning):
    """An unexpected event was caught"""


class InotifyRootDeleted(Warning):
    """A inotify root was deleted, please update the configuration"""


class InotifyRootMoved(Warning):
    """A inotify root was deleted, please update the configuration.
    Moves to other roots not supported yet"""


class InotifyTranscientPath(Warning):
    """A new path was discovered but disappeared right after"""


class InotifyDisappearingWD(Warning):
    """A watch Descriptor from inotify disappeared"""


class InotifyMissingingWD(Warning):
    """A watch Descriptor from inotify disappeared"""
