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
This module implements unit testing for retrieving calexp images in ImageServ API.

@author: Kenny Lo, SLAC

'''

import os
import unittest

# import lsst.utils.tests
from lsst.dax.imgserv.cli import ImageServCLI

def setup_module(module):
  #  lsst.utils.tests.init()
    pass

ROOT = os.path.abspath(os.path.dirname(__file__))

class ImgServTest(unittest.TestCase):
    """ Test Cases - calexp full/cutout images """
   
    #  _config = os.path.path.join(ROOT, 
    _configdir = os.path.join(ROOT, "config")
    _inputdir = os.path.join(ROOT, "input")
    _outputdir = os.path.join(ROOT, "output")

    def tearDown(self):
        pass

    def testImgServ_i6(self):
        self._run_test("test_calexp_i6.json")

    def testImgServ_i7(self):
        self._run_test("test_calexp_i7.json")

    def testImgServ_i8(self):
        self._run_test("test_calexp_i8.json")

    def testImgServ_i9(self):
        self._run_test("test_calexp_i9.json")

    def testImgServ_i10(self):
        self._run_test("test_calexp_i10.json")

    def testImgServ_i11(self):
        self._run_test("test_calexp_i11.json")

    def testImgServ_i12(self):
        self._run_test("test_calexp_i12.json")

    # run to generate small image (fits) file only
    # def testImgServ_trim(self):
    #    self._run_test("test_calexp_trim3.json")

    # common subroutine
    def _run_test(self, test_input):
        in_req = os.path.join(self._inputdir, test_input)
        cli = ImageServCLI(self._configdir, in_req, self._outputdir)
        cli.process_request()


if __name__ == "__main__":
    # lsst.utils.tests.init()
    unittest.main()

