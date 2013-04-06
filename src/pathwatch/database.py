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

    def __init__(self, database, dbhelper=None):
        if dbhelper is None:
            self._con = sqlite3.connect(database, isolation_level=None)
            self._cursor = self._con.cursor()
        else:
            # Using a cursor from another helper. It's ours, thus W0212
            self._con = None
            self._cursor = dbhelper._cursor  # pylint: disable=W0212
        self.create_table()

    def create_table(self):
        """Create the table used by this helper"""
        raise NotImplementedError("Sub-Classes need to implement create table")

    def close(self):
        """Close the connection to the database"""
        if self._con is not None:
            self._con.close()


class DBRootHelper(DBHelper):
    """"Database interaction for adding/removing/listing roots"""

    def create_table(self):
        """Create the file table needed for the algorithm"""
        self._cursor.execute('CREATE TABLE IF NOT EXISTS roots ('
                             ' path TEXT NOT NULL PRIMARY KEY'
                             ')')

    def add_root(self, path):
        """Add a new root, should not be present before"""
        self._cursor.execute(('INSERT INTO roots'
                              ' (path)'
                              ' VALUES (?)'),
                             (path,))

    def is_root(self, path):
        """Test if given path is a root"""
        self._cursor.execute(('SELECT path FROM roots'
                              ' WHERE path = ?'),
                             (path,))
        return self._cursor.fetchone() is not None

    def list_roots(self):
        """List all the existing roots"""
        self._cursor.execute('SELECT path FROM roots')
        return [row[0] for row in self._cursor.fetchall()]


class DBFilesHelper(DBHelper):
    """Database interactions for adding/removing files and paths"""

    def create_table(self):
        """Create the file table needed for the algorithm"""
        DBHashHelper._create_table(self._cursor)
        self._cursor.execute('CREATE TABLE IF NOT EXISTS files ('
                             ' parent TEXT NOT NULL,'
                             ' name TEXT NOT NULL,'
                             ' mtime INTEGER,'
                             ' identity INTEGER REFERENCES hashes (id),'
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
        # TODO: Check if the dir is an existing root !
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

    def link_to_hash(self, path, rowid):
        """Link a path to a hash"""
        parent = os.path.dirname(path)
        name = os.path.basename(path)
        self._cursor.execute('UPDATE files'
                             ' SET identity = ?'
                             ' WHERE parent == ?  AND name == ?',
                             (rowid, parent, name,))
        return self._cursor.rowcount

    def get_unhashed_files(self, max_files):
        """Get at most <max> files that weren't hashed yet"""
        self._cursor.execute('SELECT parent, name FROM files'
                             ' WHERE identity IS 0 AND mtime IS NOT 0'
                             ' LIMIT ?',
                             (max_files,))
        result = []
        row = self._cursor.fetchone()
        while row is not None:
            result.append(os.path.join(row[0], row[1]))
            row = self._cursor.fetchone()
        return result

    def _list_hashes(self):
        """List all the files, with their hash_ids, for testing purpose only"""
        self._cursor.execute('SELECT parent, name, identity FROM files'
                             ' WHERE mtime IS NOT 0 AND identity IS NOT 0')
        result = {}
        row = self._cursor.fetchone()
        while row is not None:
            result[os.path.join(row[0], row[1])] = row[2]
            row = self._cursor.fetchone()
        return result

    def _list_hashes_join(self):
        """List all the files, with their hash_ids, for testing purpose only"""
        self._cursor.execute('SELECT f.parent,f.name,h.crc,h.e2dk'
                             ' FROM files AS f NATURAL LEFT JOIN hashes AS h'
                             ' WHERE mtime IS NOT 0')
        result = {}
        row = self._cursor.fetchone()
        while row is not None:
            result[os.path.join(row[0], row[1])] = row[2:]
            row = self._cursor.fetchone()
        return result

    def _get_full_content(self):
        """Fetch the whole content of the database, for testing purpose only"""
        self._cursor.execute('SELECT parent, name, mtime from files')
        real_database = {}
        row = self._cursor.fetchone()
        while row is not None:
            real_database[os.path.join(row[0], row[1])] = row[2]
            row = self._cursor.fetchone()
        return real_database


class DBHashHelper(DBHelper):
    """Database interactions for adding/removing hash on known files"""

    @staticmethod
    def _create_table(cursor):
        """Create the file table needed for the storing hash"""
        cursor.execute('CREATE TABLE IF NOT EXISTS hashes ('
                       ' id INTEGER PRIMARY KEY,'
                       ' crc TEXT NOT NULL,'
                       ' e2dk TEXT NOT NULL,'
                       ' content INTEGER,'
                       ' upstream INTEGER,'
                       ' UNIQUE (crc, e2dk)'
                       ')')

    def create_table(self):
        """Create the file table needed for the storing hash"""
        DBHashHelper._create_table(self._cursor)

    def _get_id(self, e2dk, crc):
        """Return the id of a given hash"""
        self._cursor.execute('SELECT id FROM hashes'
                             ' WHERE e2dk = ? AND crc = ?',
                             (e2dk, crc,))
        return self._cursor.fetchone()

    def insert_hash(self, e2dk, crc):
        """Insert a new hash into the database"""
        try:
            self._cursor.execute('INSERT INTO hashes'
                                 ' (e2dk, crc) '
                                 ' VALUES (?, ?)',
                                 (e2dk, crc,))
            return self._cursor.lastrowid
        except sqlite3.IntegrityError as sqlie:
            rowid = self._get_id(e2dk, crc)
            if rowid is None:
                raise sqlie
            else:
                return rowid[0]

    def _get_full_content(self):
        """Fetch the whole content of the database, for testing purpose only"""
        self._cursor.execute('SELECT id, e2dk, crc from hashes')
        real_database = {}
        row = self._cursor.fetchone()
        while row is not None:
            real_database[row[0]] = row[1:]
            row = self._cursor.fetchone()
        return real_database
