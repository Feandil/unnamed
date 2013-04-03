"""Test if the database helper are working as predicted or not"""

from pathwatch.database import DBRootHelper, DBFilesHelper

import unittest


class TestDBRoot(unittest.TestCase):  # IGNORE:R0904
    """Test if the DBRootHelper is working as predicted or not"""

    def setUp(self):  # IGNORE:C0103
        """Create an in-memory database"""
        self._db = DBRootHelper(':memory:')
        self._db.create_table()
        self.expected_roots = []

    def tearDown(self):  # IGNORE:C0103
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


class TestFilesRoot(unittest.TestCase):  # IGNORE:R0904
    """Test if the DBFilesHelper is working as predicted or not"""

    def setUp(self):  # IGNORE:C0103
        """Create an in-memory database"""
        self._db = DBFilesHelper(':memory:')
        self._db.create_table()
        self.expected_db = {}

    def tearDown(self):  # IGNORE:C0103
        """Delete the in-memory database"""
        real_db = self._db._get_full_content()  # IGNORE:W0212
        self.assertDictEqual(self.expected_db, real_db)
        self._db.close()
        self._db.close()

    def test_listempty(self):
        """Simply try to create an empty database and list it"""
        pass

    # TODO: test all the functions IGNORE:W0511
