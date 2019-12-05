#!/usr/bin/env python
#
# This file is part of dax_imgserv.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
This module implements unit testing for retrieving raw images in ImageServ API.

"""

import os
import unittest

from lsst.dax.imgserv.cli import ImageServCLI


ROOT = os.path.abspath(os.path.dirname(__file__))


class ImageTest(unittest.TestCase):
    """ Test cases for full/cutout image retrieval """
    _inputdir = os.path.join(ROOT, "input")
    _outputdir = os.path.join(ROOT, "output")
    _cli = ImageServCLI( _outputdir)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_raw(self):
        for filename in os.listdir(self._inputdir):
            if "raw" in filename:
                pass  # self._run_test(filename)

    def test_calexp(self):
        for filename in os.listdir(self._inputdir):
            if "calexp" in filename:
                self._run_test(filename)

    def test_deepcoadd(self):
        for filename in os.listdir(self._inputdir):
            if "deepcoadd" in filename:
                self._run_test(filename)

    def _run_test(self, test_input):
        in_req = os.path.join(self._inputdir, test_input)
        self._cli.process_request(in_req)


if __name__ == "__main__":
    unittest.main()
