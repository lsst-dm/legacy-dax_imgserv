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

import logging as log

import lsst.afw
import lsst.afw.coord as afwCoord
import lsst.afw.geom as afwGeom
import lsst.afw.image as afwImage
import lsst.afw.math as afwMath

from lsst.imgserv.imageStitch import stitchExposuresGoodPixelCopy

def getSkyMap(ctrCoord, width, height, filt, source, mapType, patchType):
    '''Merge multiple patches from a SkyMap into a single image.
    This function takes advantage of the fact that all tracts in a SkyMap share
    the same WCS and should have identical pixels where they overlap.
    @ ctrCoord: base coordinate of the box RA and Dec (minimum values, lower left corner)
    TODO: This should be changed to box center and change width and height to arcseconds? DM-2467
    @ width: in Pixel
    @ height: in Pixels
    @ filter: valid filter for the data set such as 'i', 'r', 'g'
    @ source: source for Butler, such as "/lsst7/releaseW13EP"
    @ mapType: type of SkyMap, such as "deepCoadd_skyMap"
    @ patchType: patch type for butler to retrieve, such as "deepCoadd"
    '''
    # Get the basic SkyMap information
    butler = lsst.daf.persistence.Butler(source)
    skyMap = butler.get(mapType)
    trInfo = skyMap.findTract(ctrCoord)
    destWcs = trInfo.getWcs()
    # Determine target area.
    ctrCoordPix = destWcs.skyToPixel(ctrCoord)
    p2i = afwGeom.Point2I(ctrCoordPix)
    destBBox = afwGeom.Box2I(p2i, afwGeom.Extent2I(width, height))
    destCornerCoords = [destWcs.pixelToSky(pixPos) for pixPos in  afwGeom.Box2D(destBBox).getCorners()]
    # Collect patches of the SkyMap that are in the target region.
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

        # load srcExposures
        tractId = tractInfo.getId()
        for patchInfo in patchList:
            patchIndex = patchInfo.getIndex()
            pInd = ','.join(str(i) for i in patchIndex)
            log.info("butler.get dataId=filter:{}, tract:{}, patch:{}".format(filt, tractId, pInd))
            patchExposure = butler.get("deepCoadd", dataId={"filter": filt, "tract": tractId, "patch": pInd})
            srcView = afwImage.ExposureF(srcExposure, patchExposure.getBBox())
            srcViewImg = srcView.getMaskedImage()
            patchImg = patchExposure.getMaskedImage()
            srcViewImg <<= patchImg

    # Copy the pixels from the patches into the destination exposures.
    destExposureList = []
    for srcExpo in srcExposureList:
        sImg = srcExpo.getMaskedImage()
        srcWcs = srcExpo.getWcs()
        dExpo = afwImage.ExposureF(destBBox, srcWcs)
        dImg = dExpo.getMaskedImage()
        beginX = destBBox.getBeginX() - sImg.getX0()
        endX = destBBox.getEndX() - sImg.getX0()
        beginY = destBBox.getBeginY() - sImg.getY0()
        endY = destBBox.getEndY() - sImg.getY0()
        dImg <<= sImg[beginX:endX, beginY:endY]
        destExposureList.append(dExpo)

    # If there's only one exposure in the list (and there usually is) just return it.
    if len(destExposureList) == 1:
        return  destExposureList[0]
    # Need to stitch together the multiple destination exposures.
    # TODO: locate region that exercises this code for testing DM-2467
    warperConfig = afwMath.WarperConfig()
    warper = afwMath.Warper.fromConfig(warperConfig)
    badPixelMask = afwImage.MaskU.getPlaneBitMask([])
    stitchedExpo = stitchExposuresGoodPixelCopy(destWcs, destBBox, destExposureList, warper, badPixelMask)
    #stitchedExpo.writeFits("skyCutOutStitched.fits")
    return stitchedExpo

