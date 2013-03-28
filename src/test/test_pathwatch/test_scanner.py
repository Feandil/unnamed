"""Test if the scanner is working as expected"""

import os
import unittest
import shutil
import tempfile
import time

from pathwatch.scanner import Scanner


def _get_sql_content(scanner):
    """Get the content of the database in the form of a dict"""
    curs = scanner._db._cursor  # IGNORE:W0212
    curs.execute('SELECT parent, name, mtime from files')
    real_database = {}
    row = curs.fetchone()
    while row is not None:
        real_database[os.path.join(row[0], row[1])] = row[2]
        row = curs.fetchone()
    return real_database


def _create_file(filename, database):
    """Helper for creating a file"""
    with open(filename, 'w') as fff:
        fff.write('\n')
    database[filename] = int(os.stat(filename).st_mtime)


def _create_dir(dirname, database):
    """Helper for creating a file"""
    os.mkdir(dirname)
    database[dirname] = 0


def _delete_path(path, database):
    """Helper for removing a file or a folder"""
    if os.path.isdir(path):
        os.rmdir(path)
    else:
        os.remove(path)
    del database[path]


class TestScanner(unittest.TestCase):  # IGNORE:R0904
    """Test if the Scanner is working as predicted or not"""

    def setUp(self):  # IGNORE:C0103
        """Create a Scanner with an in-memory database"""
        self.scanner = Scanner(':memory:')
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):  # IGNORE:C0103
        """Delete the internal Scanner"""
        self.scanner.close()
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_createtable(self):
        """Just create a table"""
        self.scanner.create_table()
        self.assertDictEqual({}, _get_sql_content(self.scanner))

    def test_scan_empty(self):
        """Scan an empty folder"""
        self.scanner.create_table()
        database = {self.tempdir: 0}
        self.scanner.scan(self.tempdir)
        self.assertDictEqual(database, _get_sql_content(self.scanner))

    def test_scan_empty_two_times(self):
        """Scan an empty folder"""
        self.scanner.create_table()
        database = {self.tempdir: 0}
        self.scanner.scan(self.tempdir)
        self.assertDictEqual(database, _get_sql_content(self.scanner))
        self.scanner.scan(self.tempdir)
        self.assertDictEqual(database, _get_sql_content(self.scanner))

    def test_scan_dir_two_times(self):
        """Scan a folder containing one folder"""
        self.scanner.create_table()
        database = {self.tempdir: 0}
        dir_name = os.path.join(self.tempdir, 'a')
        _create_dir(dir_name, database)
        self.scanner.scan(self.tempdir)
        self.assertDictEqual(database, _get_sql_content(self.scanner))
        self.scanner.scan(self.tempdir)
        self.assertDictEqual(database, _get_sql_content(self.scanner))

    def test_scan_dir_delete_it(self):
        """Scan a folder containing one folder and rescan after deletion"""
        self.scanner.create_table()
        database = {self.tempdir: 0}
        dir_name = os.path.join(self.tempdir, 'a')
        _create_dir(dir_name, database)
        self.scanner.scan(self.tempdir)
        self.assertDictEqual(database, _get_sql_content(self.scanner))
        _delete_path(dir_name, database)
        self.scanner.scan(self.tempdir)
        self.assertDictEqual(database, _get_sql_content(self.scanner))

    def test_scan_dir_file(self):
        """Scan a folder containing one folder and a file inside it"""
        self.scanner.create_table()
        database = {self.tempdir: 0}
        dir_name = os.path.join(self.tempdir, 'a')
        file_name = os.path.join(dir_name, 'b')
        _create_dir(dir_name, database)
        _create_file(file_name, database)
        self.scanner.scan(self.tempdir)
        self.assertDictEqual(database, _get_sql_content(self.scanner))

    def test_scan_dir_file_delete_file(self):
        """Scan a folder containing one folder and a file inside it, delete the file"""
        self.scanner.create_table()
        database = {self.tempdir: 0}
        dir_name = os.path.join(self.tempdir, 'a')
        file_name = os.path.join(dir_name, 'b')
        _create_dir(dir_name, database)
        _create_file(file_name, database)
        self.scanner.scan(self.tempdir)
        self.assertDictEqual(database, _get_sql_content(self.scanner))
        _delete_path(file_name, database)
        self.scanner.scan(self.tempdir)
        self.assertDictEqual(database, _get_sql_content(self.scanner))

    def test_scan_dir_file_delete_dir(self):
        """Scan a folder containing one folder and a file inside it, delete the folder"""
        self.scanner.create_table()
        database = {self.tempdir: 0}
        dir_name = os.path.join(self.tempdir, 'a')
        file_name = os.path.join(dir_name, 'b')
        _create_dir(dir_name, database)
        _create_file(file_name, database)
        self.scanner.scan(self.tempdir)
        self.assertDictEqual(database, _get_sql_content(self.scanner))
        _delete_path(file_name, database)
        _delete_path(dir_name, database)
        self.scanner.scan(self.tempdir)
        self.assertDictEqual(database, _get_sql_content(self.scanner))

    def test_more_files(self):
        """Scan a folder containing a complexe tree"""
        self.scanner.create_table()
        dir_name_1 = os.path.join(self.tempdir, 'a')
        dir_name_2 = os.path.join(self.tempdir, 'b')
        file_name_1 = os.path.join(dir_name_1, 'c')
        file_name_2 = os.path.join(dir_name_2, 'd')
        dir_name_3 = os.path.join(dir_name_2, 'e')
        file_name_3 = os.path.join(dir_name_3, 'f')
        database = {self.tempdir: 0}
        for dir_name in [dir_name_1, dir_name_2, dir_name_3]:
            _create_dir(dir_name, database)
        for file_name in [file_name_1, file_name_2, file_name_3]:
            _create_file(file_name, database)
        self.scanner.scan(self.tempdir)
        self.assertDictEqual(database, _get_sql_content(self.scanner))

    def test_more_files_delete_path(self):
        """Scan a folder containing a complexe tree"""
        self.scanner.create_table()
        dir_name_1 = os.path.join(self.tempdir, 'a')
        dir_name_2 = os.path.join(self.tempdir, 'b')
        file_name_1 = os.path.join(dir_name_1, 'c')
        file_name_2 = os.path.join(dir_name_2, 'd')
        dir_name_3 = os.path.join(dir_name_2, 'e')
        file_name_3 = os.path.join(dir_name_3, 'f')
        database = {self.tempdir: 0}
        for dir_name in [dir_name_1, dir_name_2, dir_name_3]:
            _create_dir(dir_name, database)
        for file_name in [file_name_1, file_name_2, file_name_3]:
            _create_file(file_name, database)
        self.scanner.scan(self.tempdir)
        self.assertDictEqual(database, _get_sql_content(self.scanner))
        _delete_path(file_name_3, database)
        _delete_path(dir_name_3, database)
        _delete_path(file_name_2, database)
        _delete_path(dir_name_2, database)
        self.scanner.scan(self.tempdir)
        self.assertDictEqual(database, _get_sql_content(self.scanner))

    def test_transfort_file_path(self):
        """Transform a file into a folder with a file inside"""
        self.scanner.create_table()
        database = {self.tempdir: 0}
        file_dir_name = os.path.join(self.tempdir, 'a')
        file_name = os.path.join(file_dir_name, 'b')
        _create_file(file_dir_name, database)
        self.scanner.scan(self.tempdir)
        self.assertDictEqual(database, _get_sql_content(self.scanner))
        _delete_path(file_dir_name, database)
        _create_dir(file_dir_name, database)
        _create_file(file_name, database)
        self.scanner.scan(self.tempdir)
        self.assertDictEqual(database, _get_sql_content(self.scanner))

    def test_transfort_path_file(self):
        """Transform a folder with a file inside into a file"""
        self.scanner.create_table()
        database = {self.tempdir: 0}
        file_dir_name = os.path.join(self.tempdir, 'a')
        file_name = os.path.join(file_dir_name, 'b')
        _create_dir(file_dir_name, database)
        _create_file(file_name, database)
        self.scanner.scan(self.tempdir)
        self.assertDictEqual(database, _get_sql_content(self.scanner))
        _delete_path(file_name, database)
        _delete_path(file_dir_name, database)
        _create_file(file_dir_name, database)
        self.scanner.scan(self.tempdir)
        self.assertDictEqual(database, _get_sql_content(self.scanner))

    def test_modify_file(self):
        """Modify a file a check if the mtime was updated"""
        self.scanner.create_table()
        database = {self.tempdir: 0}
        file_name = os.path.join(self.tempdir, 'a')
        _create_file(file_name, database)
        self.scanner.scan(self.tempdir)
        self.assertDictEqual(database, _get_sql_content(self.scanner))
        time.sleep(1)
        _delete_path(file_name, database)
        _create_file(file_name, database)
        self.scanner.scan(self.tempdir)
        self.assertDictEqual(database, _get_sql_content(self.scanner))
