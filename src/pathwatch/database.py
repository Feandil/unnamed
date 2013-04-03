# ----------------------------------------------------------------------------
# "THE BEER-WARE LICENSE" (Revision 42):
# <git@lerya.net> wrote this file. As long as you retain this notice you can
# do whatever you want with this stuff. If we meet some day, and you think
# this stuff is worth it, you can buy me a beer in return. Vincent Brillault
# ----------------------------------------------------------------------------

"""Database interactions for scanner & inotify thread"""

import sqlite3
import os


class DBHelper(object):
    """Base class for other helpers"""

    def __init__(self, database):
        self._con = sqlite3.connect(database, isolation_level=None)
        self._cursor = self._con.cursor()
        self.create_table()

    def create_table(self):
        """Create the table used by this helper"""
        raise NotImplementedError("Sub-Classes need to implement create table")

    def close(self):
        """Close the connection to the database"""
        self._con.close()

class DBFilesHelper(DBHelper):
    """Database interactions for adding/removing files and paths"""

    def create_table(self):
        """Create the file table needed for the algorithm"""
        self._cursor.execute('CREATE TABLE files ('
                             ' parent TEXT NOT NULL,'
                             ' name TEXT NOT NULL,'
                             ' mtime INTEGER,'
                             ' identity INTEGER,'
                             ' PRIMARY KEY (parent, name)'
                             ')')

    def get_path(self, path):
        """Get the information about a file/folder"""
        parent = os.path.dirname(path)
        name = os.path.basename(path)
        self._cursor.execute(('SELECT mtime FROM files'
                              ' WHERE parent = ? AND name = ?'),
                             (parent, name,))
        return self._cursor.fetchone()

    def list_path(self, path):
        """List the content found in a folder"""
        self._cursor.execute(('SELECT name, mtime FROM files'
                              ' WHERE parent == ?'), (path,))
        files = {}
        dirs = set()
        row = self._cursor.fetchone()
        while row is not None:
            if row[1] == 0:
                dirs.add(row[0])
            else:
                files[row[0]] = row[1]
            row = self._cursor.fetchone()
        return (files, dirs)

    def insert_file(self, path, mtime):
        """Insert a new file"""
        parent = os.path.dirname(path)
        name = os.path.basename(path)
        self._cursor.execute(('INSERT INTO files'
                              ' (mtime, parent, name, identity)'
                              ' VALUES (?, ?, ? ,0)'),
                             (mtime, parent, name,))

    def insert_dir(self, path):
        """Insert a new folder"""
        self.insert_file(path, 0)

    def insert_files(self, new_data):
        """Insert a bunch of files"""
        if len(new_data) == 0:
            return
        self._cursor.executemany(('INSERT INTO files'
                                  ' (mtime, parent, name, identity)'
                                  ' VALUES (?, ?, ? ,0)'),
                                 iter(new_data))

    def insert_dirs(self, root, names):
        """Insert a bunch of files"""
        if len(names) == 0:
            return
        self._cursor.executemany(('INSERT INTO files'
                                  ' (mtime, parent, name, identity)'
                                  ' VALUES (0, ?, ? ,0)'),
                                 iter([(root, name) for name in names]))

    def update_file(self, path, mtime):
        """Update the mtime information about a file"""
        parent = os.path.dirname(path)
        name = os.path.basename(path)
        self._cursor.execute(('UPDATE files SET mtime = ?, identity = 0'
                              ' WHERE parent == ? AND name == ?'),
                             (mtime, parent, name,))

    def update_files(self, new_data):
        """Update the mtime information about a bunch of files"""
        if len(new_data) == 0:
            return
        self._cursor.executemany(('UPDATE files SET mtime = ?, identity=0'
                                  ' WHERE parent == ? AND name == ?'),
                                 iter(new_data))

    def delete_single(self, path):
        """Delete a single file/folder"""
        parent = os.path.dirname(path)
        name = os.path.basename(path)
        self._cursor.execute(('DELETE FROM files'
                              ' WHERE parent == ?  AND name == ?'),
                             (parent, name,))

    def delete_path(self, path):
        """Delete the whole tree under a path"""
        self._cursor.execute('DELETE FROM files'
                             ' WHERE parent = ? OR parent LIKE ?',
                             (path, os.path.join(path, '%'),))
        self.delete_single(path)

    def delete_singles(self, root, names):
        """Remove a bunch of outdated paths"""
        for name in names:
            self._cursor.execute(('DELETE FROM files'
                                  ' WHERE parent == ?  AND name == ?'),
                                 (root, name,))

    def delete_paths(self, root, names):
        """Remove a bunch of outdated paths"""
        for name in names:
            path = os.path.join(root, name)
            self._cursor.execute('DELETE FROM files'
                                 ' WHERE parent = ? OR parent LIKE ?',
                                 (path, os.path.join(path, '%'),))
        self.delete_singles(root, names)

    def _get_full_content(self):
        """Fetch the whole content of the database, for testing purpose only"""
        self._cursor.execute('SELECT parent, name, mtime from files')
        real_database = {}
        row = self._cursor.fetchone()
        while row is not None:
            real_database[os.path.join(row[0], row[1])] = row[2]
            row = self._cursor.fetchone()
        return real_database
