# ----------------------------------------------------------------------------
# "THE BEER-WARE LICENSE" (Revision 42):
# <git@lerya.net> wrote this file. As long as you retain this notice you can
# do whatever you want with this stuff. If we meet some day, and you think
# this stuff is worth it, you can buy me a beer in return. Vincent Brillault
# ----------------------------------------------------------------------------

"""Scans filesystems from an entry point, populating the database"""

import os

from .database import DBFilesHelper


class Scanner(object):
    """Scans filesystems from an entry point, populating the database

        Need to be the only one that insert/delete rows in the database when
        running: no concurrency protection (Other threads/processes can still
        try to update those values, but need to handle row suppression)
    """

    def __init__(self, database):
        self._db = DBFilesHelper(database)
        self._db.create_table()

    def close(self):
        """Close the scanner (close the underlying sqlite connection)"""
        self._db.close()

    def _scan_file(self, path):
        """Scan and update a given file"""
        row = self._db.get_path(path)
        if row is not None and row[0] == 0:
            # Was a dir, clean it
            self._db.delete_path(path)
        if not os.path.exists(path):
            # Was a file but does not exists anymore, clean
            self._db.delete_single(path)
            return
        mtime = int(os.stat(path).st_mtime)
        if row is None:
            self._db.insert_file(path, mtime)
        elif row[0] < mtime:
            self._db.update_file(path, mtime)

    def _scan_folder(self, path):
        """Scan the tree under path (a folder). Does not follow symlinks"""
        # Check if it was a file before
        row = self._db.get_path(path)
        if row is None:
            self._db.insert_dir(path)
        elif not row[0] == 0:
            self._db.delete_single(path)
        # Walk (does not follow symlinks
        for (root, dirs, files) in os.walk(path):
            # Extract old data
            (old_files, old_dirs) = self._db.list_path(root)
            # Remove old dirs
            self._db.delete_paths(root, old_dirs - set(dirs))
            # Remove old files
            self._db.delete_singles(root,
                                    set(old_files.keys()) - set(files))
            # Create new folders
            self._db.insert_dirs(root, set(dirs) - old_dirs)
            # Update files
            insert_file = []
            update_file = []
            for new_file in files:
                new_mtime = int(os.stat(os.path.join(root, new_file)).st_mtime)
                try:
                    old_mtime = old_files[new_file]
                    if old_mtime < new_mtime:
                        update_file.append((new_mtime, root, new_file))
                except KeyError:
                    insert_file.append((new_mtime, root, new_file))
            self._db.insert_files(insert_file)
            self._db.update_files(update_file)

    def scan(self, path):
        """Scan a path, file or folder"""
        if not os.path.exists(path):
            self._db.delete_path(path)
            return
        if os.path.isdir(path):
            self._scan_folder(path)
        else:
            self._scan_file(path)

    def scan_file(self, path):
        """Scan a  file (abort if folder)"""
        if not os.path.exists(path):
            self._db.delete_path(path)
            return
        if not os.path.isdir(path):
            self._scan_file(path)
