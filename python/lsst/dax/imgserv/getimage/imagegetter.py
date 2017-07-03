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
#
#
# This code is used to to select an image or a cutout of an image
# that has its center closest to the specified RA and Dec. The
# image is retrieved using the Data Butler.

"""
This library module is used to locate and retrieve variolus image types and cutout 
dimensions, via the appropriate Butler object passed in. 

@author: John Gates, SLAC
@author: Brian Van Klaveren, SLAC
@author: Kenny W. Lo, SLAC

"""

import gzip
import math
import os
import sys
import time

import lsst.afw
import lsst.afw.coord as afwCoord
import lsst.afw.display as afwDisplay
import lsst.afw.geom as afwGeom
import lsst.afw.image as afwImage

import lsst.log as log


class ImageGetter:
    """Provide operations to retrieve images including cutouts from the specified 
    image repository through the passed-in butler and metaserv.

    """
    
    def __init__(self, butlerget, metaservget, logger):
        """ Instantiate ImageGetter object with butler, butler configuration,
        and connection for image metadata.

        Parameters
        ----------
        butlerget : ButlerGet 
            the butler instance and config info.
        metaservget : MetaservGet
            provides access to image metadata.
        logger : log
            the logger to be used.

        """
        self._log = logger
        self._butler = butlerget.butler
        self._butler_keys = butlerget.butler_keys
        self._imagedataset_type = butlerget.butler_policy
        self._metaservget = metaservget

    def data_id_from_request(self, request):
        """Returns the data ID, a dictionary of key value pairs, from the request with
        valid being true if there were entries for everything in butler keys.
        This will not work for floating point values.

        """
        valid = True
        ids = {}
        for key in self._butler_keys:
            value = request.args.get(key)
            if value is None:
                valid = False
            try:
                value = int(value)
            except:
                value = str(value)
            ids[key] = value
        return ids, valid

    def image_by_data_id(self, data_id):
        """Retrieve and image from the butler by the image id values in the dictionary ids
        The needed values are specified in _butler_keys.
        
        """
        img = self._butler.get(self._imagedataset_type, dataId=data_id)
        return img 

    def fullimage(self, ra, dec, filtername):
        """Return an image containing ra and dec with filtername (optional)
        Returns None if no image is found.
        This function assumes the entire image is valid. (no overscan, etc.)
        
        """
        res = self._metaservget.nearest_image_containing(ra, dec, filtername)
        img = self._imagefrombutler(res)
        return img

    def image_cutout(self, ra, dec, filtername, width, height, cutout_type="arcsecond"):
        """Return an image centered on ra and dec (in degrees) with dimensions
        height and width (in arcseconds).
        - Use filtername, ra, dec, width, and height to find an image from the database.
        
        """
         # Find the nearest image to ra and dec.
        self._log.debug("getImage %f %f %f %f", ra, dec, width, height)
        qresult = self._metaservget.nearest_image_containing(ra, dec, filtername)
        return self._imagecutout_by_data_id(ra, dec, width, height, qresult, cutout_type)
   
    def imagecutout_from_science_id(self, scienceID, ra, dec, width, height, units):
        """ Get the image specified by id centered on (ra, dec) with width and height dimensions.
        Units (or cutout_type): "arcsecond", "pixel"
        
        """
        # Get the corresponding image(data) id from the butler
        dataID, valid = self.data_id_from_science_id(scienceID)
        self._log.debug("imagecutout_from_science_id dataID:{}".format(dataID))
        if valid:
            # make id compatible with qResult type via custom wrapping
            c_qr = ["CUSTOM_QR", dataID]
            image = self._imagecutout_by_data_id(ra, dec, width, height, c_qr, units)
            return image

    def data_id_from_science_id(self, science_id):
        """Returns a dictionary of ids derived from scienceId.
        The ids match the ids in _butler_keys and valid is false
        if at least one of the ids is missing.
        
        """
        valid = True
        ids = {}
        science_id = int(science_id)
        if self._butler_keys == ["run", "camcol", "field", "filter"]:
            possibleFields = {
                "field": science_id % 10000,
                "camcol": (science_id//10000)%10,
                "filter": "ugriz"[(science_id//100000)%10],
                "run": science_id//1000000,
            }
            self._log.debug("w13Db data_id_from_science_id {}".format( 
                possibleFields))
            for key in self._butler_keys:
                value = possibleFields[key]
                if value is None:
                    valid = False
                ids[key] = value
            self._log.debug("W13Db ids={} {}".format(valid, ids))
        elif self._butler_keys == ["tract", "patch", "filter"]:
            patchY = (science_id//8)%(2**13)
            patchX = (science_id//(2**16))%(2**13)
            possibleFields = {
                "filter": "ugriz"[science_id%8],
                "tract": science_id//(2**29),
                "patch": "%d,%d" % (patchX, patchY)
            }
            self._log.debug("w13DeepCoaddDb: data_id_from_science_id {}".format( 
                possibleFields))
            for key in self._butler_keys:
                value = possibleFields[key]
                if value is None:
                    valid = False
                ids[key] = value
            self._log.debug("W13DeepCoaddDb dataID={} {}".format(valid, ids))
        return ids, valid 
 
    def apply_cutout(self, src_img, metadata, ra, dec, width, height, qresults,
            cutout_type="arcsecond"):
        """Return an image centered on ra and dec (in degrees) with dimensions
        height and width (in arcseconds by default).

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
        
        Returns
        -------
        afwImage  
                the cutout image 
        
        """
        self._log.debug("getImage %f %f %f %f", ra, dec, width, height)
        imgW = src_img.getWidth()
        imgH = src_img.getHeight()
        self._log.debug("imgW=%d imgH=%d", imgW, imgH)
        wcs = lsst.afw.image.makeWcs(metadata, False)
        raDec = afwCoord.makeCoord(afwCoord.ICRS,
                                   ra * afwGeom.degrees,
                                   dec * afwGeom.degrees)
        xyWcs = wcs.skyToPixel(raDec)
        x0, y0 = src_img.getX0(), src_img.getY0()
        xyCenter_x = xyWcs.getX() - x0
        xyCenter_y = xyWcs.getY() - y0
        self._log.info("ra=%f dec=%f xyWcs=(%f,%f) x0y0=(%f,%f) xyCenter=(%f,%f)", ra, 
                dec, xyWcs.getX(), xyWcs.getY(), x0, y0, xyCenter_x, xyCenter_y)
        if cutout_type == 'pixel':
            img = self._cutoutbox_pixels(src_img, xyCenter_x, xyCenter_y, 
                    width, height, wcs, self._log)
            return img
        # Determine approximate pixels per arcsec - find image corners in RA and Dec
        # and compare that distance with the number of pixels.
        raDecUL = wcs.pixelToSky(afwGeom.Point2D(0, 0))
        raDecLR = wcs.pixelToSky(afwGeom.Point2D(imgW - 1, imgH - 1))
        self._log.debug("raDecUL 0=%f 1=%f", raDecUL[0].asDegrees(), raDecUL[1].asDegrees())
        self._log.debug("raDecLR 0=%f 1=%f", raDecLR[0].asDegrees(), raDecLR[1].asDegrees())
        # length of a line from upper left (UL) to lower right (LR)
        decDist = raDecUL[1].asArcseconds() - raDecLR[1].asArcseconds()
        raLR = self._keep_within_180(raDecUL[0].asDegrees(), raDecLR[0].asDegrees())
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
        self._log.info("ra=%f dec=%f xyWcs=(%f,%f) x0y0=(%f,%f) xyCenter=(%f,%f)", ra,
                dec, xyWcs.getX(), xyWcs.getY(), x0, y0, xyCenter_x, xyCenter_y)
        img = self._cutoutbox_pixels(src_img, xyCenter_x, xyCenter_y, pixW, pixH, wcs, self._log)
        return img

    def _imagecutout_by_data_id(self, ra, dec, width, height, qresults, cutout_type="arcsecond"):
        # Return an image by data ID through the butler.
        img = self._imagefrombutler(qresults)
        if img is None:
            # @todo html error handling see DM-1980
            return None
        # Get the metadata for the source image.
        metadata = self._metadata_from_data_id(qresults)
        img_co = self.apply_cutout(img, metadata, ra, dec, width, height,
                qresults, cutout_type)
        return img_co

    def _metadata_from_data_id(self, qresults):
        # Return the metadata for the query results in qResults and a butler.
        id_type, keyvals = self._data_id_from_qr(qresults)
        if id_type == "RCFF":
            run, camcol, field, filtername = keyvals 
            return self._butler.get(self._imagedataset_md(), run=run, camcol=camcol,
                    field=field, filter=filtername)
        elif id_type == "TPF":
            tract, patch, filtername = keyvals
            return self._butler.get(self._imagedataset_md(), 
                    tract=tract, patch=patch, filter=filtername)
               
    def _imagefrombutler(self, qresults):
        # Retrieve the image through the Butler for this image type using the
        # query results as in 'qresults'.
        self._log.debug("_imagefrombutler qResults:{}".format(qresults))
        id_type, keyvals = self._data_id_from_qr(qresults)
        if id_type == "RCFF": # rcff=run, camcol, field, filter
            run, camcol, field, filtername = keyvals
            log.debug("_imagefrombutler run={} camcol={} field={} \
                    filter={}".format(run, 
                camcol, field, filtername))
            img = self._butler.get(self._imagedataset_type, run=run, camcol=camcol,
                    field=field, filter=filtername)
            return img 
        elif id_type == "TPF": # tpf=tract, patch, filtername
            tract, patch, filtername = keyvals
            self._log.debug("deepCoadd _imagefrombutler tract={} patch={} \
                    filtername={}".format(tract, patch, filtername))
            img = self._butler.get(self._imagedataset_type, tract=tract,
                    patch=patch, filter=filtername)
            return img

    def _imagedataset_md(self):
        #Return the butler policy name to retrieve metadata
        return self._imagedataset_type + "_md"

    def _cutoutbox_pixels(self, src_image, xyCenter_x, xyCenter_y, width, height, wcs, log):
        #Returns an image cutout from the source image.
        #   srcImage - Source image.
        #   xyCenter - The center of region to cutout in pixels.
        #   width - The width in pixels.
        #   height - The height in pixels.
        #   height and width will be trimmed if they go past the edge of the source image.
        # First, center the cutout image
        pixULX = int(xyCenter_x - width/2.0)
        pixULY = int(xyCenter_y - height/2.0)
        xyCenter = afwGeom.Point2I(pixULX, pixULY)
        log.debug("xyCenter={}".format(xyCenter))
        src_box = src_image.getBBox()
        # assuming both src_box and xyCenter to be in Box2I 
        co_box = afwGeom.Box2I(xyCenter, afwGeom.Extent2I(int(width), int(height)))
        co_box.clip(src_box)
        if co_box.isEmpty():
            return None 
        pixULX = co_box.getBeginX()
        pixEndX = co_box.getEndX()
        pixULY = co_box.getBeginY()
        pixEndY = co_box.getEndY()
        log.debug("co_box pixULX={} pixEndX={} pixULY={} pixEndY={}".format(pixULX,
            pixEndX, pixULY, pixEndY))
        if isinstance(src_image, afwImage.ExposureF):
            img= afwImage.ExposureF(src_image, co_box)
            img.setWcs(wcs)
        else:
            # for non-ExposureF, e.g. raw (DecoratedImage)
            img = src_image[pixULX:pixEndX, pixULY:pixEndY].clone()
        return img

    def _keysfromlist(self, flist, fields):
        #flist presumed to be a dictionary;fields, an array.
        vals= []
        for f in fields:
            vals.append(flist.get(f))
        return vals

    def _data_id_from_qr(self, qresults):
        # identify and fetch the data ID from 1 of 2 defined sets
        cqr = False
        ln = qresults[0]
        if ln == "CUSTOM_QR":
            cqr = True
            ln = qresults[1]
        # first try run, camcol, field, filter keys
        if self._butler_keys == ["run", "camcol", "field", "filter"]: 
            if cqr:
                run, camcol, field, filtername = self._keysfromlist(ln,
                ['run', 'camcol', 'field', 'filter'])
            else:
                run, camcol, field, filtername = ln[2:6]
            return "RCFF", [run, camcol, field, filtername]
        # if no match, then try tract, patch, filter keys
        if self._butler_keys == ["tract", "patch", "filter"]:
            if cqr:
                tract, patch, filtername = self._keysfromlist(ln, ["tract",
                    "patch", "filter"])
            else:
                tract, patch, filtername = ln[2:5]
            return "TPF", [tract, patch, filtername]
     
    def _arcsec_to_deg(self, arcsecs):
        return float(arcsecs/3600.0)

    def _keep_within_180(self, target, val):
        # Return a value that is equivalent to val on circle
        # within 180 degrees of target.
        while val > (target + 180.0):
            val -= 360.0
        while val < (target - 180.0):
            val += 360.0
        return val



