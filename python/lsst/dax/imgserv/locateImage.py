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

@author: John Gates, SLAC; Kenny Lo, SLAC
'''

import gzip
import math
import os
import sys
import time

from sqlalchemy.exc import SQLAlchemyError

import lsst.afw
import lsst.afw.coord as afwCoord
import lsst.afw.display as afwDisplay
import lsst.afw.geom as afwGeom
import lsst.afw.image as afwImage
import lsst.daf.base as dafBase
import lsst.daf.persistence as dafPersist
import lsst.log as log
from lsst.db.engineFactory import getEngineFromFile
from lsst.obs.sdss import sdssMapper


class W13Db:
    '''Base class for examining DC_W13_Stripe82 data
    '''
    def __init__(self, credFileName, database, table, columns, dataRoot, butlerPolicy, butlerKeys, logger):
        self._log = logger
        self._table = table
        self._columns = columns
        self._conn = getEngineFromFile(credFileName, database=database).connect()
        self._dataRoot = dataRoot
        self._imageDatasetType = butlerPolicy
        self._butlerKeys = butlerKeys
        sql = "SET time_zone = '+0:00'"
        try:
            self._log.info(sql)
            self._conn.execute(sql)
        except SQLAlchemyError as e:
            self._log.error("Db engine error %s" % e)

    def getImageDatasetMd(self):
        '''Return the butler policy name to retrieve metadata
        '''
        return self._imageDatasetType + "_md"

    def getIdsFromRequest(self, request):
        '''Returns a dictionary of key value pairs from the request with
        valid being true if there were entries for everything in _butlerKeys.
        This will not work for floating point values.
        '''
        valid = True
        ids = {}
        for key in self._butlerKeys:
            value = request.args.get(key)
            if value is None:
                valid = False
            try:
                value = int(value)
            except:
                value = str(value)
            ids[key] = value
        return ids, valid

    def getImageIdsFromScienceId(self, scienceId):
        '''Returns a dictionary of ids derived from scienceId.
        The ids match the ids in _butlerKeys and valid is false
        if at least one of the ids is missing.
        '''
        valid = True
        ids = {}
        scienceId = int(scienceId)
        possibleFields = {
            "field": scienceId % 10000,
            "camcol": (scienceId//10000)%10,
            "filter": "ugriz"[(scienceId//100000)%10],
            "run": scienceId//1000000,
        }

        for key in self._butlerKeys:
            value = possibleFields[key]
            if value is None:
                valid = False
            ids[key] = value
        return ids, valid

    def getImageByIds(self, ids):
        '''Retrieve and image from the butler by the image id values in the dictionary ids
        The needed values are specified in butlerKeys.'''
        butler = lsst.daf.persistence.Butler(self._dataRoot)
        img = butler.get(self._imageDatasetType, dataId=ids)
        return img, butler

    def getImageFull(self, ra, dec, filterName):
        '''Return an image containing ra and dec with filterName (optional)
        Returns None if no image is found.
        This function assumes the entire image is valid. (no overscan, etc.)
        '''
        img, metadata = self.getImageFullWithMetadata(ra, dec, filterName)
        return img

    def getImageFullWithMetadata(self, ra, dec, filterName):
        '''Return an image containing ra, dec, and filterName (optional) with corresponding metadata.
        Returns None if no image is found.
        This function assumes the entire image is valid. (no overscan, etc.)
        '''
        res = self._findNearestImageContaining(ra, dec, filterName)
        img, butler = self._getImageButler(res)
        metadata = self._getMetadata(butler, res)
        return img, metadata

    def getImage(self, ra, dec, filterName, width, height, cutoutType="arcsecond"):
        '''Return an image centered on ra and dec (in degrees) with dimensions
        height and width (in arcseconds).
        - Use filterName, ra, dec, width, and height to find an image from the database.
        '''
         # Find the nearest image to ra and dec.
        self._log.debug("getImage %f %f %f %f", ra, dec, width, height)
        qresult = self._findNearestImageContaining(ra, dec, filterName)
        return self.getImageByDataId(ra, dec, width, height, qresult, cutoutType)

    def getImageByDataId(self, ra, dec, width, height, qResults, cutoutType="arcsecond"):
        '''Return an image centered on ra and dec (in degrees) with dimensions
        height and width (in arcseconds).
        Returns None if no image is found.
        This function assumes the entire image is valid. (no overscan, etc.)
        Sequence of events:
        - dataId is the image id for the butler
        - Use the results of the query to get an image and metadata from the butler.
        - Map ra, dec, width, and height to a box.
        - If a pixel cutout, trim the dimesions to fit in the source image and return.
        -     and return the cutout.
        - Otherwise, the height and width are in arcseconds.
        - Determine approximate pixels per arcsecond in the image by
             calculating the length of line from the upper right corner of
             the image to the lower left corner in pixels and arcseconds.
             (This will fail at or very near the pole.)
        - Use that to define a box for the cutout.
        - Trim the box so it is entirely within the source image.
        - Make and return the cutout.
        '''
        self._log.debug("getImage %f %f %f %f", ra, dec, width, height)
        img, butler = self._getImageButler(qResults)
        if img is None:
            # @todo html error handling see DM-1980
            return None
        imgW = img.getWidth()
        imgH = img.getHeight()
        self._log.debug("imgW=%d imgH=%d", imgW, imgH)
        # Get the metadata for the source image.
        metadata = self._getMetadata(butler, qResults)
        wcs = lsst.afw.image.makeWcs(metadata, False)
        raDec = afwCoord.makeCoord(afwCoord.ICRS,
                                   ra * afwGeom.degrees,
                                   dec * afwGeom.degrees)
        xyWcs = wcs.skyToPixel(raDec)
        x0 = img.getX0()
        y0 = img.getY0()
        xyCenter = afwGeom.Point2I(int(xyWcs.getX() - x0), int(xyWcs.getY() - y0))
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
        raLR *= 3600.0  # convert degrees to arcseconds
        # Correct distance in RA for the declination
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

    def _findNearestImageContaining(self, ra, dec, filterName):
        '''Use the ra, dec, and filterName (optional) to find the image with its
        center nearest ra and dec. It returns the result of the SQL query.
        '''
        cols = ["ra", "decl"]
        for s in self._columns:
            cols.append(s)
        dist = "(power((ra - {}),2) + power((decl - {}),2)) as distance".format(ra, dec)
        # More accurate distance calc on a sphere-if needed
        # SELECT *, 2 * ASIN(SQRT(POWER(SIN((raA)*pi()/180/2),2)+
        # COS(raA*pi()/180)*COS(abs(raB)*pi()/180)*
        # POWER(SIN((decB.lon)*pi()/180/2),2)) as distance
        # FROM <table> order by distance ;
        filterSql = ""
        if filterName:
            filterSql = "filterName = '{}' AND".format(filterName)
        cols.append(dist)
        col_str = ",".join(cols)
        sql = ("SELECT {} FROM {} WHERE {} "
               "scisql_s2PtInBox({}, {}, corner1Ra, corner1Decl, corner3Ra, corner3Decl) = 1 "
               "order by distance LIMIT 1").format(col_str, self._table, filterSql, ra, dec)
        self._log.info(sql)
        log.info("findNearest sql={}".format(sql))
        return self._conn.execute(sql).fetchall()


class W13RawDb(W13Db):
    '''This class is used to connect to the DC_W13_Stripe82 Raw database.
    Raw images
    ----------
    Repository path: /datasets/sdss/preprocessed/dr7/runs
    Butler keys: run, camcol, field, filter
    MySQL table: DC_W13_Stripe82.Science_Ccd_Exposure
    Table columns: run, camcol, field, filterName
    butler.get("raw", run=run, camcol=camcol, field=field, filter=filterName)
    '''
    def __init__(self, credFileName, logger=log):
        # @todo The names needed for the data butler need to come from a central location.
        W13Db.__init__(self,
                       credFileName,
                       database="DC_W13_Stripe82",
                       table="Science_Ccd_Exposure",
                       columns=["run", "camcol", "field", "filterName"],
                       dataRoot="/datasets/sdss/preprocessed/dr7/runs",
                       butlerPolicy="fpC",
                       butlerKeys=["run", "camcol", "field", "filter"],
                       logger=logger)

    def _getImageButler(self, qResults):
        '''Retrieve the image and butler for this image type using the query results in 'qResults'
        The retrieval process varies for different image types.
        '''
        # This will return on the first result.
        log.debug("Raw_getImageButler qResults:{}".format(qResults))
        valid, run, camcol, field, filterName = _getKeysForButler(qResults)
        if valid is True:
            log.debug("Raw_getImageButler run={} camcol={} field={} filter={}".format(run, 
                camcol, field, filterName))
            butler = lsst.daf.persistence.Butler(self._dataRoot)
            img = butler.get(self._imageDatasetType, run=run, camcol=camcol, field=field, filter=filterName)
            return img, butler
        else:
            return None, None


    def _getMetadata(self, butler, qResults):
        '''Return the metadata for the query results in qResults and a butler.
        '''
        for ln in qResults:
            run, camcol, field, filterName = ln[2:6]
            return butler.get(self.getImageDatasetMd(), run=run, camcol=camcol,
                              field=field, filter=filterName)


    def _getImageCutoutFromScienceId(self, scienceId, ra, dec, width, height, units):
        ''' Get the image specified by id centered on (ra, dec) with width and height dimensions.
        Units (or cutoutType): "arcsecond", "pixel"
        '''
        # Get the corresponding image(data) id from the butler
        dataId, valid = self.getImageIdsFromScienceId(scienceId)
        log.debug("Raw_getImageCutoutFromScienceId dataId:{}".format(dataId))
        if valid is True:
            # make id compatible with qResult type via custom wrapping
            c_qr = ['CUSTOM_QR', dataId]
            image = self.getImageByDataId(ra, dec, width, height, c_qr, units)
            return image
        else:
            return None


class W13CalexpDb(W13RawDb):
    '''This class is used to connect to the DC_W13_Stripe82 Calibration Exposures.
    Calibration Exposures look to be very similar to retrieving Raw exposres. Once
    this is shown to work, W13CalebDb and W13RawDb should be refactored to have a
    commnon base class and add a field for policy "fpC" or "calexp".
    ----------
    Repository path: /datasets/sdss/preprocessed/dr7/sdss_stripe82_00/calexps/
    Butler keys: run, camcol, field, filter
    MySQL table: DC_W13_Stripe82.Science_Ccd_Exposure
    Table columns: run, camcol, field, filterName
    butler.get("calexp", run=run, camcol=camcol, field=field, filter=filterName)
    '''
    def __init__(self, credFileName, logger=log):
        # @todo The names needed for the data butler need to come from a central location.
        W13Db.__init__(self,
                       credFileName,
                       database="DC_W13_Stripe82",
                       table="Science_Ccd_Exposure",
                       columns=["run", "camcol", "field", "filterName"],
                       dataRoot="/datasets/sdss/preprocessed/dr7/sdss_stripe82_00/calexps",
                       butlerPolicy="calexp",
                       butlerKeys=["run", "camcol", "field", "filter"],
                       logger=logger)
    
    def _getMetadata(self, butler, qResults):
        '''Return the metadata for the query results in qResults and a butler.
        '''
        valid, run, camcol, field, filterName = _getKeysForButler(qResults)
        return butler.get(self.getImageDatasetMd(), run=run, camcol=camcol, 
                field=field, filter=filterName)


class W13DeepCoaddDb(W13Db):
    '''This class is used to connect to the DC_W13_Stripe82 Coadd database.
    Coadd images
    ------------
    Repository path: "/datasets/sdss/preprocessed/dr7/sdss_stripe82_00/coadd"
    Butler keys: tract, patch, filter
    MySQL table: DC_W13_Stripe82.DeepCoadd
    Table columns: tract, patch, filterName
    butler.get("deepCoadd", filter=filterName, tract=tract, patch=patch)
    '''
    def __init__(self, credFileName, logger=log):
        # @todo The names needed for the data butler need to come from a central location.
        W13Db.__init__(self,
                       credFileName,
                       database="DC_W13_Stripe82",
                       table="DeepCoadd",
                       columns=["tract", "patch", "filterName"],
                       dataRoot="/datasets/sdss/preprocessed/dr7/sdss_stripe82_00/coadd",
                       butlerPolicy="deepCoadd",
                       butlerKeys=["tract", "patch", "filter"],
                       logger=logger)

    def getImageIdsFromScienceId(self, scienceId):
        '''Returns a dictionary of ids derived from scienceId.
        The ids match the ids in _butlerKeys and valid is false
        if at least one of the ids is missing.
        '''
        valid = True
        ids = {}
        scienceId = int(scienceId)
        patchY = (scienceId//8)%(2**13)
        patchX = (scienceId//(2**16))%(2**13)
        possibleFields = {
            "filter": "ugriz"[scienceId%8],
            "tract": scienceId//(2**29),
            "patch": "%d,%d" % (patchX, patchY)
        }
        for key in self._butlerKeys:
            value = possibleFields[key]
            if value is None:
                valid = False
            ids[key] = value
        return ids, valid

    def _getImageButler(self, qResults):
        '''Retrieve the image and butler for this image type using the query results in 'qResults'
        The retrieval process varies for different image types.
        '''
        # This will return on the first result.
        log.debug("Raw_getImageButler qResults:{}".format(qResults))
        valid, tract, patch, filterName = _getKeysForButler2(qResults)
        if valid is True:
            log.debug("deepCoad _getImageButler getting butler tract={} patch={} filterName={}".format(
                      tract, patch, filterName))
            butler = lsst.daf.persistence.Butler(self._dataRoot)
            img = butler.get(self._imageDatasetType, tract=tract, patch=patch, filter=filterName)
            return img, butler
        else:
            return None, None

    def _getMetadata(self, butler, qResults):
        '''Return the metadata for the query results in qResults and a butler
        '''
        for ln in qResults:
            tract = ln[2]
            patch = ln[3]
            filterName = ln[4]
            metadata = butler.get(self.getImageDatasetMd(), tract=tract, patch=patch, filter=filterName)
            return metadata


    def _getImageCutoutFromScienceId(self, scienceId, ra, dec, width, height, units):
        ''' Get the image specified by id centered on (ra, dec) with width and height dimensions.
        Units (or cutoutType): arcsecond, pixel
        '''
        # Get the corresponding image(data) id from the butler
        dataId, valid = self.getImageIdsFromScienceId(scienceId)
        log.debug("DeepCoadd getImageCutoutFromScienceId dataId:{}".format(dataId))
        if valid is True:
            # make id compatible with qResult type via custom wrapping
            c_qr = ['CUSTOM_QR', dataId]
            image = self.getImageByDataId(ra, dec, width, height, c_qr, units)
            return image
        else:
            return None

    
def _cutoutBoxPixels(srcImage, xyCenter, width, height, log):
    '''Returns an image cutout from the source image.
    srcImage - Source image.
    xyCenter - The center of region to cutout in pixels.
    width - The width in pixels.
    height - The height in pixels.
    height and width will be trimmed if they go past the edge of the source image.
    '''
    # assuming both src_box and xyCenter to be in Box2I 
    log.debug("xyCenter={}".format(xyCenter))
    src_box = srcImage.getBBox()
    co_box = afwGeom.Box2I(xyCenter, afwGeom.Extent2I(int(width), int(height)))
    co_box.clip(src_box)
    pixULX = co_box.getBeginX()
    pixEndX = co_box.getEndX()
    pixULY = co_box.getBeginY()
    pixEndY = co_box.getEndY()
    log.debug("co_box pixULX={} pixEndX={} pixULY={} pixEndY={}".format(pixULX,
        pixEndX, pixULY, pixEndY))
    if co_box.isEmpty():
        return None                  
    imgSub = srcImage[pixULX:pixEndX, pixULY:pixEndY].clone()
    return imgSub


def _getKeysFromList(flist, fields):
    '''flist assumed to be dictionary
    '''
    vals = []
    for f in fields:
        vals.append(flist.get(f))
    return vals


def _getKeysForButler(qResults):
    valid, hm = False, False
    for ln in qResults:
        if ln == 'CUSTOM_QR':   # custom tag
            hm = True
            continue 
        elif hm is True:
            run, camcol, field, filterName = _getKeysFromList(ln,
                    ['run', 'camcol', 'field', 'filter'])
            valid = True
        else:
            run, camcol, field, filterName = ln[2:6]
            valid = True
    return valid, run, camcol, field, filterName


def _getKeysForButler2(qResults):
    valid, hm = False, False
    for ln in qResults:
        if ln == 'CUSTOM_QR':   # custom tag
            hm = True
            continue 
        elif hm is True:
            tract, patch, filterName = _getKeysFromList(ln,
                    ['tract', 'patch', 'filter'])
            valid = True
        else:
            tract, patch, filterName = ln[2:5]
            valid = True
    return valid, tract, patch, filterName


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


def dbOpen(credFileName, W13db, logger=log):
    '''Open a database connection and return an instance of the
    class indicated by W13db.
    '''
    return W13db(credFileName)


