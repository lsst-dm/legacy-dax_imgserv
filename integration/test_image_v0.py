#!/usr/bin/env python
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

from lsst.dax.imgserv.cli import ImageServCLI


ROOT = os.path.abspath(os.path.dirname(__file__))

RAW_ENDPOINTS = ["1", "2", "3", "4", "5", "18", "19"]

CALEXP_ENDPOINTS = ["6", "7", "8", "9", "10", "11", "12"]

COADD_ENDPOINTS = ["13", "14", "15", "16", "17", "20", "21"]

RAW_FILE_PATTERN = "test_raw_i{endpoint}.json"
CALEXP_FILE_PATTERN = "test_calexp_i{endpoint}.json"
COADD_FILE_PATTERN = "test_deepcoadd_i{endpoint}.json"


class ImgServTest(unittest.TestCase):
    """ Test cases for full/cutout image retrieval """

    _configdir = os.path.join(ROOT, "config")
    _inputdir = os.path.join(ROOT, "input")
    _outputdir = os.path.join(ROOT, "output")

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_raw(self):
        print("Starting raw test cases...")
        for endpoint in RAW_ENDPOINTS:
            test_input = RAW_FILE_PATTERN.format(endpoint=endpoint)
            self._run_test(test_input)

    def test_calexp(self):
        print("Starting calexp test cases...")
        for endpoint in CALEXP_ENDPOINTS:
            test_input = CALEXP_FILE_PATTERN.format(endpoint=endpoint)
            self._run_test(test_input)

    def test_deepcoadd(self):
        print("Starting deepcoadd test cases...")
        for endpoint in COADD_ENDPOINTS:
            test_input = COADD_FILE_PATTERN.format(endpoint=endpoint)
            self._run_test(test_input)

    # common subroutine
    def _run_test(self, test_input):
        in_req = os.path.join(self._inputdir, test_input)
        cli = ImageServCLI(self._configdir, in_req, self._outputdir)
        cli.process_request()


if __name__ == "__main__":
    unittest.main()
