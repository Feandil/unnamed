"""Test if the RemoveScheduler is working as predicted or not"""

import unittest
import time

from pathwatch import RemoveScheduler


class TestRemoveScheduler(unittest.TestCase):  # IGNORE:R0904
    """Test if the RemoveScheduler is working as predicted or not"""

    def setUp(self):  # IGNORE:C0103
        """Create a RemoveScheduler"""
        self.dict = {'flag':'Not Empty'}
        self.scheduler = RemoveScheduler(self.dict)
        self.scheduler.start()

    def tearDown(self):  # IGNORE:C0103
        """Delete the internal scheduler"""
        self.scheduler.stop()
        self.assertTrue('flag' in self.dict)
        self.assertFalse('noflag' in self.dict)

    def test_add(self):
        """Verify that one item is correctly removed"""
        self.dict['test_add'] = 'Not Working'
        self.scheduler.add(5, 'test_add')
        self.assertTrue('test_add' in self.dict)
        time.sleep(6)
        self.assertFalse('test_add' in self.dict)

    def test_doubleadd(self):
        """Verify that two items are correctly removed"""
        self.dict['test_doubleadd_1'] = 'Not Working'
        self.dict['test_doubleadd_2'] = 'Not Working'
        self.scheduler.add(5, 'test_doubleadd_1')
        self.scheduler.add(5, 'test_doubleadd_2')
        self.assertTrue('test_doubleadd_1' in self.dict)
        self.assertTrue('test_doubleadd_2' in self.dict)
        time.sleep(6)
        self.assertFalse('test_doubleadd_1' in self.dict)
        self.assertFalse('test_doubleadd_2' in self.dict)

    def test_cancel(self):
        """Verify that one item is correctly removed"""
        self.dict['test_cancel'] = 'Working'
        self.scheduler.add(5, 'test_add')
        self.scheduler.cancel('test_add')
        time.sleep(6)
        self.assertTrue('test_cancel' in self.dict)
        del self.dict['test_cancel']

    def test_halfcancel(self):
        """Verify that one item is correctly removed and the other one kept"""
        self.dict['test_halfcancel_keep'] = 'Working'
        self.dict['test_halfcancel_remove'] = 'Not Working'
        self.scheduler.add(5, 'test_halfcancel_keep')
        self.scheduler.add(5, 'test_halfcancel_remove')
        self.scheduler.cancel('test_halfcancel_keep')
        self.assertTrue('test_halfcancel_keep' in self.dict)
        self.assertTrue('test_halfcancel_remove' in self.dict)
        time.sleep(6)
        self.assertTrue('test_halfcancel_keep' in self.dict)
        self.assertFalse('test_halfcancel_remove' in self.dict)
        del self.dict['test_halfcancel_keep']

    def test_ordering(self):
        """Verify that objects are removed in the correct order"""
        self.dict['test_ordering_first'] = 'Working'
        self.dict['test_ordering_second'] = 'Not Working'
        self.scheduler.add(10, 'test_ordering_first')
        self.scheduler.add(5, 'test_ordering_second')
        self.assertTrue('test_ordering_first' in self.dict)
        self.assertTrue('test_ordering_second' in self.dict)
        time.sleep(6)
        self.assertTrue('test_ordering_first' in self.dict)
        self.assertFalse('test_ordering_second' in self.dict)
        time.sleep(6)
        self.assertFalse('test_ordering_first' in self.dict)
        self.assertFalse('test_ordering_second' in self.dict)

    def test_setcancelset(self):
        """Verify that we can safely set-cancel-set the same item"""
        self.dict['test_setcancelset'] = 'mmmm'
        self.scheduler.add(10, 'test_setcancelset')
        self.scheduler.cancel('test_setcancelset')
        self.scheduler.add(5, 'test_setcancelset')
        self.assertTrue('test_setcancelset' in self.dict)
        time.sleep(6)
        self.assertFalse('test_setcancelset' in self.dict)

    def test_errorondoubleadd(self):
        """Verify that we cannot add the same thing twice"""
        self.dict['test_twice'] = 'mmmm'
        self.scheduler.add(5, 'test_twice')
        try:
            self.scheduler.add(10, 'test_twice')
            self.fail('KeyError exception not raised')
        except KeyError:
            pass
        self.assertRaises(KeyError, lambda (x, y): self.scheduler.add(x, y),
                          (10, 'test_twice'))
        self.assertTrue('test_twice' in self.dict)
        time.sleep(6)
        self.assertFalse('test_twice' in self.dict)
