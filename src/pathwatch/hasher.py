# ----------------------------------------------------------------------------
# "THE BEER-WARE LICENSE" (Revision 42):
# <git@lerya.net> wrote this file. As long as you retain this notice you can
# do whatever you want with this stuff. If we meet some day, and you think
# this stuff is worth it, you can buy me a beer in return. Vincent Brillault
# ----------------------------------------------------------------------------

"""Hashes files that haven't been hashed yet, store results in the database"""

import hashlib
import threading
import zlib

from .database import DBHashHelper, DBFilesHelper

_MD4 = hashlib.new('MD4')
E2DK_BLOCK = 9728000


def _get_md4():
    """Get a MD4 hashing function"""
    return _MD4.copy()


def _crc_and_e2dk(filename):
    """Compute the CRC and e2dk hash of a given file, can raise an IOError"""
    with open(filename, 'rb') as input_file:
        crc = 0
        e2dk = []
        for buf in iter(lambda: input_file.read(E2DK_BLOCK), ""):
            crc = zlib.crc32(buf, crc)
            md4 = _get_md4()
            md4.update(buf)
            e2dk.append(md4)
        final_crc = '{:0>8x}'.format(crc & 0xFFFFFFFF)
        if len(e2dk) == 0:
            raise IOError("Empty file")
        elif len(e2dk) == 1:
            final_e2dk = e2dk[0].hexdigest()
        else:
            final_md4 = _get_md4()
            for md4 in e2dk:
                final_md4.update(md4.digest())
            final_e2dk = final_md4.hexdigest()
        return (final_crc, final_e2dk)


class Hasher(threading.Thread):
    """Threaded process that gets files to hash from the database and
    hash them"""

    def __init__(self, database, files_per_call=10):
        super(Hasher, self).__init__()
        self._wakeup = threading.Condition()
        self.files_per_call = files_per_call
        self._db = database
        self._end = threading.Event()

    def run(self):
        """Get files to hash from the database, sleep if there are none"""
        hashdb = DBHashHelper(self._db)
        filedb = DBFilesHelper(None, hashdb)
        while not self._end.is_set():
            to_be_hashed = filedb.get_unhashed_files(self.files_per_call)
            for filename in to_be_hashed:
                try:
                    (crc, e2dk) = _crc_and_e2dk(filename)
                    rowid = hashdb.insert_hash(e2dk, crc)
                    filedb.link_to_hash(filename, rowid)
                except IOError:
                    filedb.delete_path(filename)
                if self._end.is_set():
                    break
            if len(to_be_hashed) == 0:
                with self._wakeup:
                    self._wakeup.wait()

    def notify(self):
        """Notify about new files to hash"""
        with self._wakeup:
            self._wakeup.notify_all()

    def stop(self):
        """Notify the underlying thread to stop, join it"""
        self._end.set()
        self.notify()
        self.join()
