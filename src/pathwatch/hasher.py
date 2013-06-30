# ----------------------------------------------------------------------------
# "THE BEER-WARE LICENSE" (Revision 42):
# <git@lerya.net> wrote this file. As long as you retain this notice you can
# do whatever you want with this stuff. If we meet some day, and you think
# this stuff is worth it, you can buy me a beer in return. Vincent Brillault
# ----------------------------------------------------------------------------

"""Hashes files that haven't been hashed yet, store results in the database"""

import hashlib
import os.path
import threading
import zlib

from database import PathFile, HashFile

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

    def __init__(self, factory, files_per_call=10):
        super(Hasher, self).__init__()
        self._wakeup = threading.Condition()
        self.files_per_call = files_per_call
        self._factory = factory
        self._end = threading.Event()

    def run(self):
        """Get files to hash from the database, sleep if there are none"""
        session = self._factory.get(expire_on_commit=False)
        while not self._end.is_set():
            query = session.query(PathFile).filter_by(hash=None)
            to_be_hashed = query.limit(self.files_per_call).all()
            session.commit()
            for pathfile in to_be_hashed:
                fullpath = os.path.join(pathfile.path, pathfile.name)
                query = session.query(PathFile).filter_by(id=pathfile.id)
                try:
                    (crc, e2dk) = _crc_and_e2dk(fullpath)
                    newhash = HashFile(crc=crc, e2dk=e2dk)
                    session.add(newhash)
                    session.commit()
                    query.update({'hash_id': newhash.id},
                                 synchronize_session=False)
                    session.commit()
                except IOError:
                    query.delete(synchronize_session=False)
                    session.commit()
                if self._end.is_set():
                    break
            if len(to_be_hashed) == 0:
                if self._end.is_set():
                    break
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
