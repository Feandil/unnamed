"""PathWatch tests"""

from .test_database import (TestDBRoot, TestDBFiles,
                            TestDBHahes, TestDBFilesHahes)
from .test_scanner import TestScanner
from .test_remove_scheduler import TestRemoveScheduler
from .test_inotify_interface import TestInotifyWatch
from .test_hasher import TestHashes, TestHasher
