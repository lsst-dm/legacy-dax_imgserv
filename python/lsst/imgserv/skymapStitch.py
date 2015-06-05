#!/usr/bin/env python

# LSST Data Management System
# Copyright 2015 AURA/LSST.
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

"""
Code to stitch together SkyMap images.

@author  John Gates, SLAC
"""
from __future__ import division

import logging as log

import lsst.afw
import lsst.afw.coord as afwCoord
import lsst.afw.geom as afwGeom
import lsst.afw.image as afwImage
import lsst.afw.math as afwMath

from lsst.imgserv.imageStitch import stitchExposuresGoodPixelCopy

def getSkyMap(ctrCoord, width, height, filt, units, source, mapType, patchType):
    '''Merge multiple patches from a SkyMap into a single image.
    This function takes advantage of the fact that all tracts in a SkyMap share
    the same WCS and should have identical pixels where they overlap.
    @ctrCoord: base coordinate of the box RA and Dec (minimum values, lower left corner)
    @width: in Pixel
    @height: in Pixels
    @filter: valid filter for the data set such as 'i', 'r', 'g'
    @units: 'pixel' or 'arcsecond' (defaults to 'pixel')
    @source: source for Butler, such as "/lsst7/releaseW13EP"
    @mapType: type of SkyMap, such as "deepCoadd_skyMap"
    @patchType: patch type for butler to retrieve, such as "deepCoadd"
    '''
    # Get the basic SkyMap information
    butler = lsst.daf.persistence.Butler(source)
    skyMap = butler.get(mapType)
    trInfo = skyMap.findTract(ctrCoord)
    destWcs = trInfo.getWcs()
    # Determine target area.
    if (units != 'arcsecond' and units != 'pixel'):
        units = 'pixel'
    destBBox = getBBoxForCoords(destWcs, ctrCoord, width, height, units)
    destCornerCoords = [destWcs.pixelToSky(pixPos) for pixPos in  afwGeom.Box2D(destBBox).getCorners()]
    # Collect patches of the SkyMap that are in the target region. Create source exposures from
    # the patches within each tract as all patches from a tract share a WCS.
    srcExposureList = []
    tractPatchList = skyMap.findTractPatchList(destCornerCoords)
    for j, tractPatch in enumerate(tractPatchList):
        tractInfo = tractPatch[0]
        patchList = tractPatch[1]
        log.info("tractInfo[{}]={}".format(j, tractInfo))
        log.info("patchList[{}]={}".format(j, patchList))
        srcWcs = tractInfo.getWcs()
        srcBBox = afwGeom.Box2I()
        for patchInfo in patchList:
            srcBBox.include(patchInfo.getOuterBBox())
        srcExposure = afwImage.ExposureF(srcBBox, srcWcs) # blank, so far
        srcExposureList.append(srcExposure)

        # load srcExposures with patches
        tractId = tractInfo.getId()
        for patchInfo in patchList:
            patchIndex = patchInfo.getIndex()
            pInd = ','.join(str(i) for i in patchIndex)
            log.info("butler.get dataId=filter:{}, tract:{}, patch:{}".format(filt, tractId, pInd))
            patchExposure = butler.get("deepCoadd", dataId={"filter": filt, "tract": tractId, "patch": pInd})
            srcView = afwImage.ExposureF(srcExposure, patchExposure.getBBox())
            srcViewImg = srcView.getMaskedImage()
            patchImg = patchExposure.getMaskedImage()
            srcViewImg[:] = patchImg

    # Copy the pixels from the source exposures to the destination exposures.
    destExposureList = []
    for j, srcExpo in enumerate(srcExposureList):
        sImg = srcExpo.getMaskedImage()
        srcWcs = srcExpo.getWcs()
        if j == 0:
            dBBox = destBBox # destBBox only correct for first image
        else:
            # Determine the correct BBox (in pixels) for the current srcWcs
            llCorner = afwGeom.Point2I(srcWcs.skyToPixel(destCornerCoords[0]))
            urCorner = afwGeom.Point2I(srcWcs.skyToPixel(destCornerCoords[2]))
            # Handle negative values for in dBBox.
            if llCorner.getX() < 0:
                llCorner.setX(0)
            if llCorner.getY() < 0:
                llCorner.setY(0)
            if urCorner.getX() < 0:
                urCorner.setX(0)
                log.warn("getSkyMap negative X for urCorner");
            if urCorner.getY() < 0:
                urCorner.setY(0)
                log.warn("getSkyMap negative Y for urCorner");
            dBBox = afwGeom.Box2I(llCorner, urCorner)
        log.info("j={} dBBox={} sBBox={}".format(j, dBBox, srcExpo.getBBox()))
        dExpo = afwImage.ExposureF(dBBox, srcWcs)
        dImg = dExpo.getMaskedImage()
        beginX = dBBox.getBeginX() - sImg.getX0()
        endX = dBBox.getEndX() - sImg.getX0()
        beginY = dBBox.getBeginY() - sImg.getY0()
        endY = dBBox.getEndY() - sImg.getY0()

        newWidth = srcExpo.getBBox().getEndX() - dBBox.getBeginX()
        newHeight = srcExpo.getBBox().getEndY() - dBBox.getBeginY()

        # Do a final check to make sure that the we're not going past the end of sImg.
        sImgLenX = sImg.getWidth()
        if endX > sImgLenX:
            newWidth = sImgLenX - beginX
            endX = sImgLenX
        sImgLenY = sImg.getHeight()
        if endY > sImgLenY:
            newWidth = sImgLenY - beginY
            endY = sImgLenY

        log.debug("beginX={} endX={}".format(beginX, endX))
        log.debug("newWidth{} = sBBox.EndX{} - sBBox.BeginX{}".format(
            newWidth, srcExpo.getBBox().getEndX(), dBBox.getBeginX()))
        log.debug("beginY={} endY={}".format(beginY, endY))
        log.debug("newHeight{} = sBBox.EndY{} - sBBox.BeginY{}".format(
            newHeight, srcExpo.getBBox().getEndY(), dBBox.getBeginY()))
        dImg[0:newWidth, 0:newHeight] = sImg[beginX:endX, beginY:endY]
        destExposureList.append(dExpo)

    # If there's only one exposure in the list (and there usually is) just return it.
    if len(destExposureList) == 1:
        return  destExposureList[0]

    # Need to stitch together the multiple destination exposures.
    log.debug("getSkyMap stitching together multiple destExposures")
    warperConfig = afwMath.WarperConfig()
    warper = afwMath.Warper.fromConfig(warperConfig)
    stitchedExpo = stitchExposuresGoodPixelCopy(destWcs, destBBox, destExposureList, warper)
    return stitchedExpo


def getBBoxForCoords(wcs, ctrCoord, width, height, units):
    '''Returns a Box2I object representing the bounding box in pixels
    of the target region.
    @wcs: WCS object for the target region.
    @ctrCoord: RA and Dec coordinate for the center of the target region.
    @width: Width of the target region with units indicated by 'units' below.
    @height: Height of the target region with units indicated by 'units' below.
    @units: Units for height and width. 'pixel' or 'arcsecond'
    '''
    bbox = afwGeom.Box2I()
    if units == 'arcsecond':
        #ctrCoord center, RA and Dec with width and height in arcseconds
        widthHalfA = afwGeom.Angle((width/2.0), afwGeom.arcseconds)
        heightHalfA = afwGeom.Angle((height/2.0), afwGeom.arcseconds)
        minRa = ctrCoord.getLongitude() - widthHalfA
        minDec = ctrCoord.getLatitude() - heightHalfA
        maxRa = ctrCoord.getLongitude() + widthHalfA
        maxDec = ctrCoord.getLatitude() + heightHalfA
        llCoord = afwCoord.Coord(minRa, minDec, ctrCoord.getEpoch())
        llCoordPix = wcs.skyToPixel(llCoord)
        urCoord = afwCoord.Coord(maxRa, maxDec, ctrCoord.getEpoch())
        urCoordPix = wcs.skyToPixel(urCoord)
        p2iMin = afwGeom.Point2I(llCoordPix)
        p2iMax = afwGeom.Point2I(urCoordPix)
        bbox = afwGeom.Box2I(p2iMin, p2iMax)
    elif units == 'pixel':
        # ctrCoord center, RA and Dec with width and height in pixels
        ctrCoordPix = wcs.skyToPixel(ctrCoord)
        minRaPix = int(ctrCoordPix.getX() - width//2)
        minDecPix = int(ctrCoordPix.getY() - height//2)
        p2i = afwGeom.Point2I(minRaPix, minDecPix)
        bbox = afwGeom.Box2I(p2i, afwGeom.Extent2I(width, height))
    else:
        raise Exception("invalid units {}".format(units))
    return bbox
