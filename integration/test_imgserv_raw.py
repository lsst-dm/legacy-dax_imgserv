#
# LSST Data Management System
# Copyright 2017 LSST/AURA.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.

'''
This module implements unit testing for retrieving raw images in ImageServ API.

@author: Kenny Lo, SLAC

'''

import os

import unittest

import lsst.utils.tests
from lsst.dax.imgserv.cli import ImageServCLI

ROOT = os.path.abspath(os.path.dirname(__file__))

def setup_module(module):
    lsst.utils.tests.init()
    

class ImgServTest(unittest.TestCase):
    """ Test Cases - raw full/cutout images """
   
   #  _config = os.path.path.join(ROOT, 
    _configdir = os.path.join(ROOT, "config")
    _inputdir = os.path.join(ROOT, "input")
    _outputdir = os.path.join(ROOT, "output")

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testImgServ_i1(self):
        self._run_test("test_raw_i1.json")
    
    def testImgServ_i2(self):
        self._run_test("test_raw_i2.json")

    def testImgServ_i3(self):
        self._run_test("test_raw_i3.json")

    def testImgServ_i4(self):
        self._run_test("test_raw_i4.json")
   
    def testImgServ_i5(self):
        self._run_test("test_raw_i5.json")

    def testImgServ_i18(self):
        self._run_test("test_raw_i18.json")

    def testImgServ_i19(self):
        self._run_test("test_raw_i19.json")

    # run to generate small image (fits) only
    # def testImgServ_trim(self):
    #     self._run_test("test_raw_trim.json")

    # common subroutine
    def _run_test(self, test_input):
        in_req = os.path.join(self._inputdir, test_input)
        cli = ImageServCLI(self._configdir, in_req, self._outputdir)
        cli.process_request()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
