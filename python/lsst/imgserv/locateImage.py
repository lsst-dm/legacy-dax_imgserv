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

import gzip
import math
import MySQLdb
import os
import sys
import time


import lsst.afw
import lsst.afw.coord as afwCoord
import lsst.afw.display as afwDisplay
import lsst.afw.geom as afwGeom
import lsst.afw.image as afwImage
import lsst.daf.base as dafBase
import lsst.log as log

from lsst.cat.dbSetup import DbSetup
from lsst.db.utils import readCredentialFile

import lsst.daf.persistence as dafPersist
from lsst.obs.sdss import sdssMapper


#Raw images
#----------
#Repository path: /lsst7/stripe82/dr7/runs
#Butler keys: run, camcol, field, filter
#MySQL table: DC_W13_Stripe82.Science_Ccd_Exposure
#Table columns: run, camcol, field, filterName
#butler.get("raw", run=run, camcol=camcol, field=field, filter=filterName)

class W13RawDb:
    '''This class is used to connect to the DC_W13_Stripe82 Raw database.
    Raw images
    ----------
    Repository path: /lsst7/stripe82/dr7/runs
    Butler keys: run, camcol, field, filter
    MySQL table: DC_W13_Stripe82.Science_Ccd_Exposure
    Table columns: run, camcol, field, filterName
    butler.get("raw", run=run, camcol=camcol, field=field, filter=filterName)
    '''
    def __init__(self, dbHost, dbPort, dbUser, dbPasswd, logger=log):
        self._log = logger
        self._database = "DC_W13_Stripe82"
        self._table = "Science_Ccd_Exposure"
        self._columns = ["run", "camcol", "field", "filterName"]
        self._connect = MySQLdb.connect(host=dbHost, port=dbPort,
                                        user=dbUser, passwd=dbPasswd,
                                        db=self._database)
        self._dataRoot = "/lsst7/stripe82/dr7/runs"
        cursor = self._connect.cursor()
        sql = "SET time_zone = '+0:00'"
        try:
            self._log.info(sql)
            cursor.execute(sql)
        except MySQLdb.Error as err:
            self._log.info("ERROR MySQL %s -- %s" % (err, crt))
        cursor.close()

    def close(self):
        self._connect.close()

    def showTables(self):
        cursor = self._connect.cursor()
        cursor.execute("SHOW TABLES")
        ret = cursor.fetchall()
        s = str(ret)
        for tbl in ret:
            cursor.execute("SHOW COLUMNS from %s" % (tbl[0]))
            s += str(cursor.fetchall())
        cursor.close()
        return s

    def getImageFull(self, ra, dec):
        '''Return an image containing ra and dec.
        This function assumes the entire image is valid. (no overscan, etc.)
        '''
        arcW = arcsecToDeg(10)
        arcH = arcsecToDeg(10)
        minRa = ra - arcW
        maxRa = ra + arcW
        minDec = dec - arcH
        maxDec = dec + arcH
        res = self._findNearestImageContaining(ra, dec, minRa, minDec, maxRa, maxDec)
        for ln in res:
            run = ln[2]
            camcol = ln[3]
            field = ln[4]
            filterName = ln[5]
            butler=lsst.daf.persistence.Butler(self._dataRoot)
            img = butler.get("fpC", run=run, camcol=camcol,
                             field=field, filter=filterName)
            return img
        return None

    def getImage(self, ra, dec, width, height):
        '''Return an image centered on ra and dec (in degrees) with dimensions
        height and width (in arcseconds).
        This function assumes the entire image is valid. (no overscan, etc.)
        '''
        self._log.debug("getImage %f %f %f %f", ra, dec, height, width)
        arcW = arcsecToDeg(width)/2.0
        arcH = arcsecToDeg(height)/2.0
        minRa = ra - arcW
        maxRa = ra + arcW
        minDec = dec - arcH
        maxDec = dec + arcH
        res = self._findNearestImageContaining(ra, dec, minRa, minDec, maxRa, maxDec)
        for ln in res:
            run = ln[2]
            camcol = ln[3]
            field = ln[4]
            filterName = ln[5]
            butler=lsst.daf.persistence.Butler(self._dataRoot)
            img = butler.get("fpC", run=run, camcol=camcol,
                             field=field, filter=filterName)
            imgW = img.getWidth()
            imgH = img.getHeight()
            self._log.debug("imgW=%d imgH=%d", imgW, imgH)
            metadata = butler.get("fpC_md", run=run, camcol=camcol,
                             field=field, filter=filterName)
            wcs = lsst.afw.image.makeWcs(metadata, False)
            raDec = afwCoord.makeCoord(afwCoord.ICRS,
                                       ra * afwGeom.degrees,
                                       dec * afwGeom.degrees)
            xyCenter = wcs.skyToPixel(raDec)
            # Determine pixels per arcsec - find image corners RA and Dec
            raDec_ul = wcs.pixelToSky(afwGeom.Point2D(0, 0))
            raDec_lr = wcs.pixelToSky(afwGeom.Point2D(imgW - 1, imgH - 1))
            self._log.debug("raDec_ul 0=%f 1=%f", raDec_ul[0].asDegrees(), raDec_ul[1].asDegrees())
            self._log.debug("raDec_lr 0=%f 1=%f", raDec_lr[0].asDegrees(), raDec_lr[1].asDegrees())
            #Approximate pixels per arcsecond
            cosDec = math.cos(dec*afwGeom.degrees)
            # length of a line from upper left to lower right
            decDist = raDec_ul[1].asArcseconds() - raDec_lr[1].asArcseconds()
            raLR = keepWithin180(raDec_ul[0].asDegrees(), raDec_lr[0].asDegrees())
            raLR *= 3600.0 # convert degrees to arcseconds
            raDist = cosDec * (raDec_ul[0].asArcseconds() - raLR)
            raDecDist = math.sqrt(math.pow(decDist, 2.0) + math.pow(raDist, 2.0))
            self._log.debug("raDecDist=%f", raDecDist)
            pixelDist = math.sqrt(math.pow(imgW, 2.0) + math.pow(imgH, 2.0))
            pixelPerArcsec = pixelDist/raDecDist
            self._log.debug("pixelPerArcsec=%f", pixelPerArcsec)
            # need minimum point and dimensions for Box2I
            pixW = width*pixelPerArcsec
            pixH = height*pixelPerArcsec
            pixULX = xyCenter.getX() - pixW/2.0
            pixULY = xyCenter.getY() - pixH/2.0
            offsetX = 0
            offsetY = 0
            if pixULX < 0:
                offsetX = pixULX
                pixULX = 0
            if pixULY < 0:
                offsetY = pixULY
                pixULY = 0
            self._log.debug("pixULX={} pixULY={} offsetX={} offsetY={}".format(pixULX, pixULY,
                                                                               offsetX, offsetY))
            # Reduce the size if it went over the edge (offsets are <= 0)
            pixW += offsetX
            pixH += offsetY
            if pixW > imgW:
                pixW = imgW
            if pixH > imgH:
                pixH = imgH
            pixULX = int(pixULX)
            pixULY = int(pixULY)
            pixW = int(pixW)
            pixH = int(pixH)
            self._log.debug("pixULX=%d pixULY=%d pixW=%d pixH=%d", pixULX, pixULY, pixW, pixH)
            #bbox = afwGeom.Box2I(afwGeom.Point2I(pixULX, pixULY),
            #                    afwGeom.Extent2I(pixW, pixH))
            #img = butler.get("fpC", run=run, camcol=camcol,
            #                 field=field, filter=filterName, bbox=bbox)
            pixEndX = pixULX + pixW
            pixEndY = pixULY + pixH
            self._log.debug("pixULX=%d pixEndX=%d, pixULY=%d pixEndY=%d",
                      pixULX, pixEndX, pixULY, pixEndY)
            # See https://lsst-web.ncsa.illinois.edu/doxygen/x_masterDoxyDoc/afw_sec_py_image.html
            imgSub = img[pixULX:pixEndX, pixULY:pixEndY].clone()
            return imgSub

    def _findNearestImageContaining(self, ra, dec, minRa, minDec, maxRa, maxDec):
        cursor = self._connect.cursor()
        cols = [ "ra", "decl" ]
        for s in self._columns:
            cols.append(s)
        dist = "(power((ra - {}),2) + power((decl - {}),2)) as distance".format(ra, dec)
        #More accurate distance calc on a sphere-
        #SELECT *, 2 * ASIN(SQRT(POWER(SIN((raA)*pi()/180/2),2)+
        #  COS(raA*pi()/180)*COS(abs(raB)*pi()/180)*
        # POWER(SIN((decB.lon)*pi()/180/2),2)) as distance
        # FROM <table> order by distance ;
        cols.append(dist)
        col_str = ",".join(cols)
        sql = ("SELECT {} FROM {} WHERE "
            "scisql_s2PtInBox(ra, decl, {}, {}, {}, {}) = 1 order by distance LIMIT 1").format(
            col_str, self._table, minRa, minDec, maxRa, maxDec)
        self._log.info(sql)
        cursor.execute(sql)
        res = cursor.fetchall()
        return res

def arcsecToDeg(arcsecs):
    return float(arcsecs/3600.0)

def dbOpen(credFileName, portDb=3306, logger=log):
    creds = readCredentialFile(credFileName, logger)
    port = portDb
    if 'port' in creds:
        port = int(creds['port'])
    w13Raw = W13RawDb(dbHost=creds['host'], dbPort=port,
                      dbUser=creds['user'], dbPasswd=creds['passwd'])
    return w13Raw

def keepWithin180(target, val):
    '''Return a value that is equivalent to val on circle
    within 180 degrees of target.
    '''
    while val > (target + 180.0):
        val -= 360.0
    while val < (target - 180.0):
        val += 360.0
    return val

def test():
    w13Raw = dbOpen("~/.mysqlAuthLSST.lsst10")
    imgFull = w13Raw.getImageFull(359.195, -0.1055)
    print "Full w={} h={}".format(imgFull.getWidth(), imgFull.getHeight())
    print "Writing imgFull.fits", imgFull
    imgFull.writeFits("imgFull.fits")
    img = w13Raw.getImage(359.195, -0.1055, 30.0, 60.0)
    print "Sub w={} h={}".format(img.getWidth(), img.getHeight())
    print "Writing img.fits", img
    img.writeFits("img.fits")

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

if __name__ == "__main__":
    log.setLevel("", log.DEBUG)
    if len(sys.argv) > 1:
        test(sys.argv)
    else:
        test()
