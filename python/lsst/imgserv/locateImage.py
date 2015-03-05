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
# This code is used to to select an image or a cutout of an image
# that has its center closest to the specified RA and Dec. The 
# image is retrieved using the Data Butler.

'''
This module is used to locate and retrieve variolus image types.

@author: John Gates, SLAC
'''

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


class W13Db:
    '''Base class for examining DC_W13_Stripe82 data
    '''
    def __init__(self, dbHost, dbPort, dbUser, dbPasswd,
                 database, table, columns, dataRoot, logger):
        self._log = logger
        self._database = database
        self._table = table
        self._columns = columns
        self._connect = MySQLdb.connect(host=dbHost, port=dbPort,
                                        user=dbUser, passwd=dbPasswd,
                                        db=self._database)
        self._dataRoot = dataRoot
        cursor = self._connect.cursor()
        sql = "SET time_zone = '+0:00'"
        try:
            self._log.info(sql)
            cursor.execute(sql)
        except MySQLdb.Error as err:
            self._log.info("ERROR MySQL %s -- %s" % (err, crt))
        cursor.close()

    def closeConnection(self):
        self._connect.close()

    def getImageFull(self, ra, dec):
        '''Return an image containing ra and dec.
        Returns None if no image is found.
        This function assumes the entire image is valid. (no overscan, etc.)
        '''
        img, metadata = self.getImageFullWithMetadata(ra, dec)
        return img

    def getImageFullWithMetadata(self, ra, dec):
        '''Return an image containing ra and dec.
        Returns None if no image is found.
        This function assumes the entire image is valid. (no overscan, etc.)
        '''
        # The SQL UDF scisql_s2PtInBox requires a box, not a point.
        # 5 arcseconds is a small arbitrary box that seems to work.
        res = self._findNearestImageContaining(ra, dec, 5, 5)
        img, butler = self._getImageButler(res)
        metadata = self._getMetadata(butler, res)
        return img, metadata

    def getImage(self, ra, dec, width, height, cutoutType="arcsecond"):
        '''Return an image centered on ra and dec (in degrees) with dimensions
        height and width (in arcseconds).
        Returns None if no image is found.
        This function assumes the entire image is valid. (no overscan, etc.)
        Sequence of events:
         - Use ra, dec, width, and height to find an image from the database.
         - Use the results of the query to get an image and metadata from the butler.
         - Map ra, dec, width, and height to a box.
         - If a pixel cutout, trim the dimesions to fit in the source image and return.
         -     and return the cutout.
         - Otherwise, the height and width are inarcseconds.
         - Determine approximate pixels per arcsecond in the image by
             calculating the length of line from the upper right corner of
             the image to the lower left corner in pixels and arcseconds.
             (This will fail at or very near the pole.)
         - Use that to define a box for the cutout.
         - Trim the box so it is entirely within the source image.
         - Make and return the cutout.
        '''
        self._log.debug("getImage %f %f %f %f", ra, dec, width, height)
        # Find the nearest image to ra and dec containing at least part of the box.
        qresult = self._findNearestImageContaining(ra, dec, width, height)
        img, butler = self._getImageButler(qresult)
        if img == None:
            # @todo html error handling see DM-1980
            return None
        imgW = img.getWidth()
        imgH = img.getHeight()
        self._log.debug("imgW=%d imgH=%d", imgW, imgH)

        # Get the metadata for the source image.
        metadata = self._getMetadata(butler, qresult)
        wcs = lsst.afw.image.makeWcs(metadata, False)
        raDec = afwCoord.makeCoord(afwCoord.ICRS,
                                   ra * afwGeom.degrees,
                                   dec * afwGeom.degrees)
        xyWcs = wcs.skyToPixel(raDec)
        x0 = img.getX0()
        y0 = img.getY0()
        xyCenter = afwGeom.Point2D(xyWcs.getX() - x0, xyWcs.getY() - y0)
        if cutoutType == 'pixel':
            imgSub = _cutoutBoxPixels(img, xyCenter, width, height, self._log)
            return imgSub

        self._log.info("ra=%f dec=%f xyWcs=(%f,%f) x0y0=(%f,%f) xyCenter=(%f,%f)", ra, dec, 
                       xyWcs.getX(), xyWcs.getY(), x0, y0, xyCenter.getX(), xyCenter.getY())
        # Determine approximate pixels per arcsec - find image corners in RA and Dec
        # and compare that distance with the number of pixels.
        raDecUL = wcs.pixelToSky(afwGeom.Point2D(0, 0))
        raDecLR = wcs.pixelToSky(afwGeom.Point2D(imgW - 1, imgH - 1))
        self._log.debug("raDecUL 0=%f 1=%f", raDecUL[0].asDegrees(), raDecUL[1].asDegrees())
        self._log.debug("raDecLR 0=%f 1=%f", raDecLR[0].asDegrees(), raDecLR[1].asDegrees())
        # length of a line from upper left (UL) to lower right (LR)
        decDist = raDecUL[1].asArcseconds() - raDecLR[1].asArcseconds()
        raLR = _keepWithin180(raDecUL[0].asDegrees(), raDecLR[0].asDegrees())
        raLR *= 3600.0 # convert degrees to arcseconds
        #Correct distance in RA for the declination
        cosDec = math.cos(dec*afwGeom.degrees)
        raDist = cosDec * (raDecUL[0].asArcseconds() - raLR)
        raDecDist = math.sqrt(math.pow(decDist, 2.0) + math.pow(raDist, 2.0))
        self._log.debug("raDecDist=%f", raDecDist)
        pixelDist = math.sqrt(math.pow(imgW, 2.0) + math.pow(imgH, 2.0))
        pixelPerArcsec = pixelDist/raDecDist
        self._log.debug("pixelPerArcsec=%f", pixelPerArcsec)
        # Need Upper Left corner and dimensions for Box2I
        pixW = width*pixelPerArcsec
        pixH = height*pixelPerArcsec
        imgSub = _cutoutBoxPixels(img, xyCenter, pixW, pixH, self._log)
        return imgSub

    def _findNearestImageContaining(self, ra, dec, width, height):
        '''Use the ra, dec, and box coordinates to find the image with its
        center nearest ra and dec. It returns the result of the SQL query.
        '''
        arcW = _arcsecToDeg(width)/2.0
        arcH = _arcsecToDeg(height)/2.0
        minRa = ra - arcW
        maxRa = ra + arcW
        minDec = dec - arcH
        maxDec = dec + arcH
        cursor = self._connect.cursor()
        cols = [ "ra", "decl" ]
        for s in self._columns:
            cols.append(s)
        dist = "(power((ra - {}),2) + power((decl - {}),2)) as distance".format(ra, dec)
        #More accurate distance calc on a sphere-if needed
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

class W13RawDb(W13Db):
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
        # @todo The names needed for the data butler need to come from a central location.
        W13Db.__init__(self,dbHost, dbPort, dbUser, dbPasswd,
                       database="DC_W13_Stripe82",
                       table="Science_Ccd_Exposure",
                       columns=["run", "camcol", "field", "filterName"],
                       dataRoot="/lsst7/stripe82/dr7/runs",
                       logger=logger)

    def _getImageButler(self, qResults):
        '''Retrieve the image and butler for this image type using the query results in 'qResults'
        The retrieval process varies for different image types.
        '''
        # This will return on the first result.
        for ln in qResults:
            run, camcol, field, filterName = ln[2:6]
            butler = lsst.daf.persistence.Butler(self._dataRoot)
            img = butler.get("fpC", run=run, camcol=camcol,
                             field=field, filter=filterName)
            return img, butler
        return None, None

    def _getMetadata(self, butler, qResults):
        '''Return the metadata for the query results in qResults and a butler.
        '''
        for ln in qResults:
            run, camcol, field, filterName = ln[2:6]
            return butler.get("fpC_md", run=run, camcol=camcol,
                              field=field, filter=filterName)

class W13DeepCoaddDb(W13Db):
    '''This class is used to connect to the DC_W13_Stripe82 Coadd database.
    Coadd images
    ------------
    Repository path: /lsst7/releaseW13EP
    Butler keys: tract, patch, filter
    MySQL table: DC_W13_Stripe82.DeepCoadd
    Table columns: tract, patch, filterName
    butler.get("deepCoadd", filter=filterName, tract=tract, patch=patch)
    '''
    def __init__(self, dbHost, dbPort, dbUser, dbPasswd, logger=log):
        # @todo The names needed for the data butler need to come from a central location.
        W13Db.__init__(self, dbHost, dbPort, dbUser, dbPasswd,
                       database="DC_W13_Stripe82",
                       table="DeepCoadd",
                       columns=["tract", "patch", "filterName"],
                       dataRoot="/lsst7/releaseW13EP",
                       logger=logger)

    def _getImageButler(self, qResults):
        '''Retrieve the image and butler for this image type using the query results in 'qResults'
        The retrieval process varies for different image types.
        '''
        # This will return on the first result.
        for ln in qResults:
            tract = ln[2]
            patch = ln[3]
            filterName = ln[4]
            butler=lsst.daf.persistence.Butler(self._dataRoot)
            img = butler.get("deepCoadd", tract=tract, patch=patch, filter=filterName)
            return img, butler
        return None, None

    def _getMetadata(self, butler, qResults):
        '''Return the metadata for the query results in qResults and a butler
        '''
        for ln in qResults:
            tract = ln[2]
            patch = ln[3]
            filterName = ln[4]
            metadata = butler.get("deepCoadd_md", tract=tract, patch=patch, filter=filterName)
            return metadata

def _cutoutBoxPixels(srcImage, xyCenter, width, height, log):
    '''Returns an image cutout from the source image.
    srcImage - Source image.
    xyCenter - The center of region to cutout in pixels.
    width - The width in pixels.
    height - The height in pixels.
    height and width will be trimmed if they go past the edge of the source image.
    '''
    pixW = width
    pixH = height
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
    log.debug("xyCenter={}".format(xyCenter))
    log.debug("pixULX={} pixULY={} offsetX={} offsetY={}".format(pixULX, pixULY,
                                                                 offsetX, offsetY))
    # Reduce the size of the box if it goes over the edge of the image (offsets are <= 0)
    pixW += offsetX
    pixH += offsetY
    imgW = srcImage.getWidth()
    imgH = srcImage.getHeight()
    pixW = min(imgW, pixW)
    pixH = min(imgH, pixH)
    pixULX = int(pixULX)
    pixULY = int(pixULY)
    pixW = int(pixW)
    pixH = int(pixH)
    log.debug("pixULX=%d pixULY=%d pixW=%d pixH=%d", pixULX, pixULY, pixW, pixH)
    #bbox = afwGeom.Box2I(afwGeom.Point2I(pixULX, pixULY),
    #                    afwGeom.Extent2I(pixW, pixH))
    #img = butler.get("fpC_sub", run=run, camcol=camcol,
    #                 field=field, filter=filterName, bbox=bbox)
    pixEndX = pixULX + pixW
    pixEndY = pixULY + pixH
    log.debug("pixULX=%d pixEndX=%d, pixULY=%d pixEndY=%d",
              pixULX, pixEndX, pixULY, pixEndY)
    # Cut the sub image out of the image. See -
    # https://lsst-web.ncsa.illinois.edu/doxygen/x_masterDoxyDoc/afw_sec_py_image.html
    imgSub = srcImage[pixULX:pixEndX, pixULY:pixEndY].clone()
    return imgSub

def _arcsecToDeg(arcsecs):
    return float(arcsecs/3600.0)

def _keepWithin180(target, val):
    '''Return a value that is equivalent to val on circle
    within 180 degrees of target.
    '''
    while val > (target + 180.0):
        val -= 360.0
    while val < (target - 180.0):
        val += 360.0
    return val

def dbOpen(credFileName, W13db, portDb=3306, logger=log):
    '''Open a database connection and return an instance of the
    class indicated by W13db.
    '''
    creds = readCredentialFile(credFileName, logger)
    port = portDb
    if 'port' in creds:
        port = int(creds['port'])
    w13db = W13db(dbHost=creds['host'], dbPort=port,
                  dbUser=creds['user'], dbPasswd=creds['passwd'])
    return w13db

