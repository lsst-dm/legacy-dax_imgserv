#!/usr/bin/env python

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

from lsst.cat.dbSetup import DbSetup
from lsst.db.testHelper import readCredentialFile
import lsst.log as log


class MetaDataFitsTest(unittest.TestCase):
    """Tests reading FITS file headers and placing them in the database.
    """

    def setUp(self):
        global _options
        pass

    def tearDown(self):
        pass

    def test_readInFits(self):
        credFile = os.path.expanduser('~/.mysqlAuthLSST')
        if not os.path.isfile(credFile):
            log.warn("Required file with credentials '%s' not found.", credFile)
            return

        testFile = ("./tests/testData/imsim_886258731_R33_S21_C12_E000.fits.gz")
        self.assertTrue(isFitsExt('stuf.fits'))
        self.assertFalse(isFitsExt('thing.txt'))
        self.assertFalse(isFitsExt('item.tx.gz'))
        self.assertTrue(isFitsExt(testFile))
        self.assertTrue(isFits(testFile))

        # Destroy existing tables and re-create them
        dbDestroyCreate(credFile, "DELETE")

        # Open a connection to the database.
        metadataFits = MetadataFitsDb(credFile)

        # test a specific file
        self.assertFalse(metadataFits.isFileInDb(testFile))
        metadataFits.insertFile(testFile)
        log.info(metadataFits.showColumnsInTables())
        self.assertTrue(metadataFits.isFileInDb(testFile))

        # test crawler
        rootDir = '~/test_md'
        rootDir = os.path.expanduser(rootDir)
        if not os.path.exists(rootDir):
            log.error("Data directory {} is required".format(rootDir))
            return
        directoryCrawl(rootDir, metadataFits)


def main():
    global _options
    unittest.main()

if __name__ == "__main__":
    log.setLevel("", log.INFO)
    main()
