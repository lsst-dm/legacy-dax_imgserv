#
# LSST Data Management System
# Copyright 2014-2015 LSST/AURA.
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
#

# The purpose of this is to read all the FITS files in a directory tree
# and insert their header information into the metadata database.

import os
import unittest

from lsst.dax.imgserv.MetadataFitsDb import dbDestroyCreate
from lsst.dax.imgserv.MetadataFitsDb import directoryCrawl
from lsst.dax.imgserv.MetadataFitsDb import isFits
from lsst.dax.imgserv.MetadataFitsDb import isFitsExt
from lsst.dax.imgserv.MetadataFitsDb import MetadataFitsDb

ROOT = os.path.abspath(os.path.dirname(__file__))
testFile = os.path.join(ROOT, 'testData',
                        'imsim_886258731_R33_S21_C12_E000.fits.gz')


class MetaDataFitsTest(unittest.TestCase):
    """Tests reading FITS file headers and placing them in the database.
    """

    def test_isFits(self):
        self.assertTrue(isFitsExt('stuf.fits'))
        self.assertFalse(isFitsExt('thing.txt'))
        self.assertFalse(isFitsExt('item.tx.gz'))
        self.assertTrue(isFitsExt(testFile))
        self.assertTrue(isFits(testFile))
        self.assertFalse(isFits("stuf.fits"))

    def test_readInFits(self):
        credFile = os.path.expanduser('~/.mysqlAuthLSST')
        if not os.path.isfile(credFile):
            raise unittest.SkipTest("Required file with credentials '{}' not found.".format(credFile))

        # Destroy existing tables and re-create them
        dbDestroyCreate(credFile, "DELETE")

        # Open a connection to the database.
        metadataFits = MetadataFitsDb(credFile)

        # test a specific file
        self.assertFalse(metadataFits.isFileInDb(testFile))
        metadataFits.insertFile(testFile)
        print(metadataFits.showColumnsInTables())
        self.assertTrue(metadataFits.isFileInDb(testFile))

        # test crawler
        rootDir = '~/test_md'
        rootDir = os.path.expanduser(rootDir)
        if not os.path.exists(rootDir):
            raise RuntimeError("Data directory {} is required".format(rootDir))
        directoryCrawl(rootDir, metadataFits)


if __name__ == "__main__":
    unittest.main()
