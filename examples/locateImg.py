#
# LSST Data Management System
# Copyright 2015 LSST/AURA.
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
#
# JGates

import sys

from lsst.imgserv.locateImage import dbOpen
import lsst.log as log

def testDefault():
    w13Raw = dbOpen("~/.mysqlAuthLSST.lsst10")
    # file format:
    # [mysql]
    #  user = <username>
    #  host = lsst10.ncsa.illinois.edu
    #  port = 3306
    #  password = <password>
    imgFull = w13Raw.getImageFull(359.195, -0.1055)
    print "Full w={} h={}".format(imgFull.getWidth(), imgFull.getHeight())
    print "Writing imgFull.fits", imgFull
    imgFull.writeFits("imgFull.fits")
    img = w13Raw.getImage(359.195, -0.1055, 30.0, 60.0)
    print "Sub w={} h={}".format(img.getWidth(), img.getHeight())
    print "Writing img.fits", img
    img.writeFits("img.fits")
    w13Raw.closeConnection()

def test(argv):
    w13Raw = dbOpen("~/.mysqlAuthLSST.lsst10")
    ra = float(argv[1])
    dec = float(argv[2])
    w = float(argv[3])
    h = float(argv[4])
    imgFull = w13Raw.getImageFull(ra, dec)
    print "Full w={} h={}".format(imgFull.getWidth(), imgFull.getHeight())
    print "Writing imgFull.fits", imgFull
    imgFull.writeFits("imgFull.fits")
    img = w13Raw.getImage(ra, dec, w, h)
    print "Sub w={} h={}".format(img.getWidth(), img.getHeight())
    print "Writing img.fits", img
    img.writeFits("img.fits")
    w13Raw.closeConnection()

if __name__ == "__main__":
    log.setLevel("", log.DEBUG)
    if len(sys.argv) > 1:
        test(sys.argv)
    else:
        testDefault()
