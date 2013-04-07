"""Test if the database helper are working as predicted or not"""

from pathwatch.database import DBRootHelper, DBFilesHelper, DBHashHelper

import os.path
import re
import unittest


class TestDBRoot(unittest.TestCase):  # pylint: disable=R0904
    """Test if the DBRootHelper is working as predicted or not"""

    def setUp(self):  # pylint: disable=C0103
        """Create an in-memory database"""
        self._db = DBRootHelper(':memory:')
        self._db.create_table()
        self.expected_roots = []

    def tearDown(self):  # pylint: disable=C0103
        """Delete the in-memory database"""
        self.assertListEqual(self.expected_roots, self._db.list_roots())
        self._db.close()

    def test_listempty(self):
        """Simply try to create an empty database and list it"""
        pass

    def test_simple_root(self):
        """Try to add a single root"""
        self._db.add_root("/a")
        self.expected_roots.append("/a")

    def test_is_root(self):
        """Try to check the is_root method"""
        self._db.add_root("/a")
        self.expected_roots.append("/a")
        self._db.add_root("/b")
        self.expected_roots.append("/b")
        self.assertTrue(self._db.is_root("/a"))
        self.assertTrue(self._db.is_root("/b"))
        self.assertFalse(self._db.is_root("/c"))


class TestDBFiles(unittest.TestCase):  # pylint: disable=R0904
    """Test if the DBFilesHelper is working as predicted or not"""

    def setUp(self):  # pylint: disable=C0103
        """Create an in-memory database"""
        self._db = DBFilesHelper(':memory:')
        self._db.create_table()
        self.expected_db = {}

    def tearDown(self):  # pylint: disable=C0103
        """Delete the in-memory database"""
        real_db = self._db._get_full_content()
        self.assertDictEqual(self.expected_db, real_db)
        self._db.close()

    def test_listempty(self):
        """Create an empty database and list it"""
        pass

    def _insert_file(self, path, mtime):
        """Helper: add a file in db and in expected db"""
        self._db.insert_file(path, mtime)
        self.expected_db[path] = mtime

    def _insert_dir(self, path):
        """Helper: add a dir in db and in expected db"""
        self._insert_file(path, 0)

    def _expected_move(self, old_root, new_root, complete_path):
        """Helper: add a move a path in expected db"""
        new_path = re.sub(old_root, new_root, complete_path)
        self.expected_db[new_path] = self.expected_db[complete_path]
        del self.expected_db[complete_path]

    def test_insert_file(self):
        """Insert a single file"""
        self._insert_file("/home/a", 42)

    def test_move_file(self):
        """Move single file"""
        path_1 = "/home/a"
        path_2 = "/home/b"
        self._insert_file(path_1, 42)
        self._db.move_file(path_1, path_2)
        self._expected_move(path_1, path_2, path_1)

    def test_move_simple_folder(self):
        """Move a folder containing a file"""
        dir_1 = "/home/a"
        dir_2 = "/home/b"
        filename = os.path.join(dir_1, 'a')
        self._insert_dir(dir_1)
        self._insert_file(filename, 42)
        self._db.move_dir(dir_1, dir_2)
        self._expected_move(dir_1, dir_2, dir_1)
        self._expected_move(dir_1, dir_2, filename)

    def test_move_recursive(self):
        """Move a folder containing a file"""
        base_dir = '/home/a'
        new_dir = '/home/b'
        first_dir = os.path.join(base_dir, 'a')
        second_dir = os.path.join(base_dir, 'b')
        third_dir = os.path.join(first_dir, 'a')
        first_file = os.path.join(first_dir, '1')
        second_file = os.path.join(second_dir, '2')
        third_file = os.path.join(third_dir, '3')
        self._insert_dir(base_dir)
        self._insert_dir(first_dir)
        self._insert_dir(second_dir)
        self._insert_dir(third_dir)
        self._insert_file(first_file, 42)
        self._insert_file(second_file, 43)
        self._insert_file(third_file, 43)
        self._db.move_dir(base_dir, new_dir)
        self._expected_move(base_dir, new_dir, base_dir)
        self._expected_move(base_dir, new_dir, first_dir)
        self._expected_move(base_dir, new_dir, second_dir)
        self._expected_move(base_dir, new_dir, third_dir)
        self._expected_move(base_dir, new_dir, first_file)
        self._expected_move(base_dir, new_dir, second_file)
        self._expected_move(base_dir, new_dir, third_file)

    # TODO: test all the functions


class TestDBHahes(unittest.TestCase):  # pylint: disable=R0904
    """Test if the DBHashHelper is working as predicted or not"""

    def setUp(self):  # pylint: disable=C0103
        """Create an in-memory database"""
        self._db = DBHashHelper(':memory:')
        self._db.create_table()
        self.expected_db = {}

    def tearDown(self):  # pylint: disable=C0103
        """Delete the in-memory database"""
        real_db = self._db._get_full_content()
        self.assertDictEqual(self.expected_db, real_db)
        self._db.close()

    def test_insert(self):
        """Try insert a hash"""
        rowid = self._db.insert_hash('1234', '123456')
        self.expected_db[rowid] = ('1234', '123456')

    def test_double_insert(self):
        """Try insert a hash two times: the 2nd should return the first id"""
        id1 = self._db.insert_hash('1234', '123456')
        self.expected_db[id1] = ('1234', '123456')
        id2 = self._db.insert_hash('1234', '123456')
        self.assertEqual(id1, id2)


class TestDBFilesHahes(unittest.TestCase):  # pylint: disable=R0904
    """Test if the interactions between DBHashHelper and DBFilesHelper are
    working as predicted or not"""

    def setUp(self):  # pylint: disable=C0103
        """Create an in-memory database"""
        self._hashdb = DBHashHelper(':memory:')
        self._hashdb.create_table()
        self.expected_hashdb = {}
        self._filedb = DBFilesHelper(None, self._hashdb)
        self._filedb.create_table()
        self.expected_filedb = {}
        self.expected_links = {}
        self.expected_unlinked = set()

    def tearDown(self):  # pylint: disable=C0103
        """Delete the in-memory database"""
        real_hashdb = self._hashdb._get_full_content()
        real_filedb = self._filedb._get_full_content()
        real_links = self._filedb._list_hashes()
        real_unlinked = self._filedb.get_unhashed_files(10)
        self.assertDictEqual(self.expected_hashdb, real_hashdb)
        self.assertDictEqual(self.expected_filedb, real_filedb)
        self.assertDictEqual(self.expected_links, real_links)
        self.assertSetEqual(self.expected_unlinked, set(real_unlinked))
        self._filedb.close()
        self._hashdb.close()

    def test_setup(self):
        """Simply try to create both tables on the same cursor"""
        pass

    def test_insert_hash_existing(self):
        """Try to link a hash to an existing path"""
        path = "/home/42"
        self._filedb.insert_file(path, 43)
        self.expected_filedb[path] = 43
        rowid = self._hashdb.insert_hash('123', '456')
        self.expected_hashdb[rowid] = ('123', '456')
        worked = self._filedb.link_to_hash(path, rowid)
        self.assertEqual(worked, 1)
        self.expected_links[path] = rowid

    def test_insert_hash_nonexisting(self):
        """Try to link a hash to an non-existing path"""
        path = "/home/42"
        rowid = self._hashdb.insert_hash('123', '456')
        self.expected_hashdb[rowid] = ('123', '456')
        worked = self._filedb.link_to_hash(path, rowid)
        self.assertEqual(worked, 0)

    def test_list_unhashed(self):
        """Try to link a hash to an non-existing path"""
        path_1 = "/home/a/1"
        path_2 = "/home/a/2"
        path_3 = "/home/b/1"
        path_4 = "/home/b/2"

        # Insert all files
        self._filedb.insert_file(path_1, 42)
        self.expected_filedb[path_1] = 42
        self.expected_unlinked.add(path_1)
        self._filedb.insert_file(path_2, 42)
        self.expected_filedb[path_2] = 42
        self.expected_unlinked.add(path_2)
        self._filedb.insert_file(path_3, 42)
        self.expected_filedb[path_3] = 42
        self.expected_unlinked.add(path_3)
        self._filedb.insert_file(path_4, 42)
        self.expected_filedb[path_4] = 42
        self.expected_unlinked.add(path_4)

        # Insert two hash
        id_1 = self._hashdb.insert_hash('123', '456')
        self.expected_hashdb[id_1] = ('123', '456')
        id_2 = self._hashdb.insert_hash('124', '457')
        self.expected_hashdb[id_2] = ('124', '457')

        # Link half hashes
        worked = self._filedb.link_to_hash(path_2, id_1)
        self.assertEqual(worked, 1)
        self.expected_links[path_2] = id_1
        self.expected_unlinked.remove(path_2)
        worked = self._filedb.link_to_hash(path_3, id_2)
        self.assertEqual(worked, 1)
        self.expected_links[path_3] = id_2
        self.expected_unlinked.remove(path_3)
