#!/usr/bin/python2

'''Run all the tests'''

import sys
import os

if len(sys.argv) > 1:
    sys.path.insert(0, sys.argv[1])
else:
    base = os.path.abspath(__file__)
    dir = os.path.dirname(base)
    target = os.path.join(dir, '..', 'src')
    clean = os.path.abspath(target)
    assert(os.path.isdir(clean))
    sys.path.insert(0, clean)

import unittest

from test_pathwatch import *

unittest.main()
