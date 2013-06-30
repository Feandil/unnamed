"""Test if the hasher is working as predicted or not"""

from pathwatch.hasher import _crc_and_e2dk, E2DK_BLOCK, Hasher
from database import PathFile, HashFile, SessionFactory

import os.path
import shutil
import time
import tempfile
import unittest


class TestHashes(unittest.TestCase):  # pylint: disable=R0904
    """Test the hash functions"""

    def setUp(self):  # pylint: disable=C0103
        """Create a temporary folder for the test"""
        self.tempdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tempdir, 'file')

    def tearDown(self):  # pylint: disable=C0103
        """Delete the temporary folder"""
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_unexistant(self):
        """Try to hash an unexistant file: IOError"""
        self.assertRaises(IOError, _crc_and_e2dk, (self.filename))

    def test_empty(self):
        """Try to hash an empty file"""
        with open(self.filename, 'w'):
            pass
        self.assertRaises(IOError, _crc_and_e2dk, (self.filename))

    def test_small_text(self):
        """Try to hash a file with a small text inside"""
        with open(self.filename, 'w') as output:
            output.write("The quick brown fox jumps over the lazy dog")
        (crc, e2dk) = _crc_and_e2dk(self.filename)
        self.assertEqual(crc, '414fa339')
        self.assertEqual(e2dk, '1bee69a46ba811185c194762abaeae90')

    def test_one_zeroed_block(self):
        """Try to hash a full e2dk block of 0"""
        with open(self.filename, 'w') as output:
            output.write('\0' * E2DK_BLOCK)
        (crc, e2dk) = _crc_and_e2dk(self.filename)
        self.assertEqual(crc, '3abc06ba')
        self.assertEqual(e2dk, 'd7def262a127cd79096a108e7a9fc138')

    def test_two_zeroed_block(self):
        """Try to hash two full e2dk block of 0"""
        with open(self.filename, 'w') as output:
            output.write('\0' * (2 * E2DK_BLOCK))
        (crc, e2dk) = _crc_and_e2dk(self.filename)
        self.assertEqual(crc, 'adccde1a')
        self.assertEqual(e2dk, '194ee9e4fa79b2ee9f8829284c466051')


class TestHasher(unittest.TestCase):  # pylint: disable=R0904
    """Test if the Hasher is working as predicted or not"""

    def _add_file(self, path, name):
        """Add a file in the underlying database"""
        self._session.add(PathFile(path=path, name=name))
        self._session.commit()
        return os.path.join(path, name)

    def setUp(self):  # pylint: disable=C0103
        """Create a temporary folder for the test, start a hasher"""
        self.tempdir = tempfile.mkdtemp()
        database = os.path.join(self.tempdir, 'test.db')
        factory = SessionFactory(database='sqlite:///' + database)
        self._session = factory.get()
        dummy_hash = HashFile(crc='13', e2dk='42')
        self._session.add(dummy_hash)
        dummy = PathFile(path='/dum', name='my', hash=dummy_hash)
        self._session.add(dummy)
        self._session.commit()
        self._hasher = Hasher(factory)
        self._hasher.start()
        self.expected_db = {'/dum/my': ('13', '42')}

    def tearDown(self):  # pylint: disable=C0103
        """Delete the temporary folder"""
        self._hasher.stop()
        real_db_data = self._session.query(PathFile, HashFile
                                           ).join(HashFile).all()
        real_db = {}
        for (pathfile, hashfile) in real_db_data:
            filename = os.path.join(pathfile.path, pathfile.name)
            real_db[filename] = (hashfile.crc, hashfile.e2dk)
        self._session.commit()
        self.assertDictEqual(self.expected_db, real_db)
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_create_start_stop(self):
        """Do nothing exept starting/stoping a daemon"""
        pass

    def test_one_file(self):
        """Hash one file"""
        filename = self._add_file(self.tempdir, 'file')
        with open(filename, 'w') as output:
            output.write('\0' * (2 * E2DK_BLOCK))
        self._hasher.notify()
        self.expected_db[filename] = ('adccde1a',
                                      '194ee9e4fa79b2ee9f8829284c466051')
        time.sleep(1)

    def test_two_file(self):
        """Hash two files"""
        filename1 = self._add_file(self.tempdir, 'file_1')
        with open(filename1, 'w') as output:
            output.write('\0' * (2 * E2DK_BLOCK))
        filename2 = self._add_file(self.tempdir, 'file_2')
        with open(filename2, 'w') as output:
            output.write('\0' * E2DK_BLOCK)
        self._hasher.notify()
        self.expected_db[filename1] = ('adccde1a',
                                       '194ee9e4fa79b2ee9f8829284c466051')
        self.expected_db[filename2] = ('3abc06ba',
                                       'd7def262a127cd79096a108e7a9fc138')
        time.sleep(1)

    def test_inexisting_file(self):
        """Try to hash an inexisting file"""
        self._add_file(self.tempdir, 'file')
        self._hasher.notify()
        time.sleep(1)
