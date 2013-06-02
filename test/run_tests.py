#!/usr/bin/python2

'''Run all the tests'''

import sys
import os

if len(sys.argv) > 1:
    sys.path.insert(0, sys.argv[1])
else:
    BASE = os.path.abspath(__file__)
    DIR = os.path.dirname(BASE)
    TARGET = os.path.join(DIR, '..', 'src')
    CLEAN = os.path.abspath(TARGET)
    assert(os.path.isdir(CLEAN))
    sys.path.insert(0, CLEAN)

import unittest

from test_pathwatch import *

unittest.main()
