"""Test if InotifyWatch is working as predicted or not"""

import unittest
import shutil
import tempfile
import time
import Queue

import os

from pathwatch import InotifyWatch

class TestInotifyWatch(unittest.TestCase):  # IGNORE:R0904
    """Test if InotifyWatch is working as predicted or not"""

    def setUp(self):  # IGNORE:C0103
        """Create a RemoveScheduler"""
        self.queue = Queue.Queue()
        self.watch = InotifyWatch(self.queue)
        self.tempdir = tempfile.mkdtemp()
        self.watch.start()

    def tearDown(self):  # IGNORE:C0103
        """Delete the internal scheduler"""
        time.sleep(1)
        self.watch.stop()
        shutil.rmtree(self.tempdir, ignore_errors=True)
        if not self.queue.empty():
            self.fail("Too many messages: {0}".format(self.queue.get()))

    def test_add_root(self):
        """Try to monitor one path"""
        self.watch.add(self.tempdir)

    def test_create_file(self):
        """Try to create a file"""
        self.watch.add(self.tempdir)
        newfile = os.path.join(self.tempdir, 'create_file')
        with open(newfile, 'w') as new_file:
            new_file.write('test')
        time.sleep(1)
        self.assertFalse(self.queue.empty())
        self.assertEqual(('modified', newfile), self.queue.get())

    def test_createremove_file(self):
        """Try to create and remove a file"""
        self.watch.add(self.tempdir)
        newfile = os.path.join(self.tempdir, 'createremove_file')
        with open(newfile, 'w') as new_file:
            new_file.write('test')
        os.remove(newfile)
        time.sleep(1)
        self.assertFalse(self.queue.empty())
        self.assertEqual(('modified', newfile), self.queue.get())
        self.assertFalse(self.queue.empty())
        self.assertEqual(('remove_file', newfile), self.queue.get())

    def test_create_dir(self):
        """Try to create  a dir"""
        self.watch.add(self.tempdir)
        newdir = os.path.join(self.tempdir, 'create_dir')
        os.mkdir(newdir)
        time.sleep(1)
        self.assertFalse(self.queue.empty())
        self.assertEqual(('new_dir', newdir), self.queue.get())

    def test_createremove_dir(self):
        """Try to create and remove a dir"""
        self.watch.add(self.tempdir)
        newdir = os.path.join(self.tempdir, 'createremove_dir')
        os.mkdir(newdir)
        time.sleep(1)
        os.rmdir(newdir)
        time.sleep(1)
        self.assertFalse(self.queue.empty())
        self.assertEqual(('new_dir', newdir), self.queue.get())
        self.assertFalse(self.queue.empty())
        self.assertEqual(('remove_dir', newdir), self.queue.get())

    def test_create_dir_and_file(self):
        """Try to create and remove a dir"""
        self.watch.add(self.tempdir)
        newdir = os.path.join(self.tempdir, 'dir')
        os.mkdir(newdir)
        newfile = os.path.join(newdir, 'file')
        time.sleep(1)
        with open(newfile, 'w') as new_file:
            new_file.write('test')
        time.sleep(1)
        self.assertFalse(self.queue.empty())
        self.assertEqual(('new_dir', newdir), self.queue.get())
        self.assertFalse(self.queue.empty())
        self.assertEqual(('modified', newfile), self.queue.get())

    def test_two_roots(self):
        """Try to create and remove a dir"""
        dir_1 = os.path.join(self.tempdir, 'root_1')
        dir_2 = os.path.join(self.tempdir, 'root_2')
        os.mkdir(dir_1)
        os.mkdir(dir_2)
        self.watch.add(dir_1)
        self.watch.add(dir_2)

    def test_movefile_between_roots(self):
        """Try to move a file between two watched areas"""
        dir_1 = os.path.join(self.tempdir, 'root_1')
        dir_2 = os.path.join(self.tempdir, 'root_2')
        pos_1 = os.path.join(dir_1, 'file')
        pos_2 = os.path.join(dir_2, 'file')
        os.mkdir(dir_1)
        os.mkdir(dir_2)
        with open(pos_1, 'w') as new_file:
            new_file.write('test')
        self.watch.add(dir_1)
        self.watch.add(dir_2)
        shutil.move(pos_1, pos_2)
        time.sleep(1)
        self.assertFalse(self.queue.empty())
        self.assertEqual(('move_file', pos_1, pos_2), self.queue.get())

    def test_movedir_between_roots(self):
        """Try to move a dir between two watched areas"""
        dir_1 = os.path.join(self.tempdir, 'root_1')
        dir_2 = os.path.join(self.tempdir, 'root_2')
        pos_1 = os.path.join(dir_1, 'dir')
        pos_2 = os.path.join(dir_2, 'dir')
        newfile = os.path.join(pos_1, 'file')
        os.mkdir(dir_1)
        os.mkdir(dir_2)
        os.mkdir(pos_1)
        with open(newfile, 'w') as new_file:
            new_file.write('test')
        self.watch.add(dir_1)
        self.watch.add(dir_2)
        shutil.move(pos_1, pos_2)
        time.sleep(1)
        self.assertFalse(self.queue.empty())
        self.assertEqual(('move_dir', pos_1, pos_2), self.queue.get())

    def test_movedir_to_root(self):
        """Try to move a dir containing a file into a watched area"""
        dir_1 = os.path.join(self.tempdir, 'root_1')
        dir_2 = os.path.join(self.tempdir, 'root_2')
        pos_1 = os.path.join(dir_1, 'dir')
        pos_2 = os.path.join(dir_2, 'dir')
        file_pos_1 = os.path.join(pos_1, 'file')
        file_pos_2 = os.path.join(pos_2, 'file')
        os.mkdir(dir_1)
        os.mkdir(dir_2)
        os.mkdir(pos_1)
        with open(file_pos_1, 'w') as new_file:
            new_file.write('test')
        self.watch.add(dir_2)
        shutil.move(pos_1, pos_2)
        time.sleep(1)
        self.assertFalse(self.queue.empty())
        self.assertEqual(('new_dir', pos_2), self.queue.get())
        self.assertFalse(self.queue.empty())
        self.assertEqual(('new_file', file_pos_2), self.queue.get())

    def test_movedir_from_root(self):
        """Try to move a dir containing a file from a watched area"""
        dir_1 = os.path.join(self.tempdir, 'root_1')
        dir_2 = os.path.join(self.tempdir, 'root_2')
        pos_1 = os.path.join(dir_1, 'dir')
        pos_2 = os.path.join(dir_2, 'dir')
        file_pos_1 = os.path.join(pos_1, 'file')
        os.mkdir(dir_1)
        os.mkdir(dir_2)
        os.mkdir(pos_1)
        with open(file_pos_1, 'w') as new_file:
            new_file.write('test')
        self.watch.add(dir_1)
        shutil.move(pos_1, pos_2)
        time.sleep(6)
        self.assertFalse(self.queue.empty())
        self.assertEqual(('remove_dir', pos_1), self.queue.get())

