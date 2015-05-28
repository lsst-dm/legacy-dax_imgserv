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
Prototype image stitching code.

@author  John Gates, SLAC
"""

import logging as log

import lsst.afw
import lsst.afw.coord as afwCoord
import lsst.afw.geom as afwGeom
import lsst.afw.image as afwImage
import lsst.afw.math as afwMath
import lsst.coadd.utils as coaddUtils
import lsst.daf.base as dafBase
import lsst.pex.config as pexConfig

from lsst.imgserv.imageStitch import CoaddConfig, stitchExposures, stitchExposuresGoodPixelCopy
from lsst.imgserv.skymapStitch import getSkyMap

# This not working. statisticsStack complains about the images being different sizes.
# Holding off for now as coadding is not particularly beneficial for skymaps. However the
# statisticsStack is believed to be supperior to coadd_utils.Coadd, so it could well
# be worth making it work for other datasets.
def stitchExposureStatisticsStack(destWcs, destBBox, expoList, warper):
    # Loosely based on pipe_tasks.assembleCoadd.
    # The weight of all images should be the same as they all come from the same skyMap.
    # Correct values for statsCtrl
    statsCtrl = afwMath.StatisticsControl()
    statsCtrl.setNumSigmaClip(3.0) # Believed to be ignored due to statsFlags = afw.Mean
    statsCtrl.setNumIter(2) # Believed to be ignored due to statsFlags = afw.Mean
    statsCtrl.setAndMask(afwImage.MaskU.getPlaneBitMask(["EDGE", "SAT"]))
    statsCtrl.setNanSafe(False) # Correct value is ???
    statsCtrl.setCalcErrorFromInputVariance(False) # Correct value is ???
    statsFlags = afwMath.MEAN
    #coaddMaskedImage = coaddExposure.getMaskedImage()
    #coaddView = afwImage.MaskedImageF(coaddMaskedImage, bbox, afwImage.PARENT, False)
    destExpo = afwImage.ExposureF(destBBox, destWcs)
    maskedImageList = afwImage.vectorMaskedImageF()
    weightList = []
    for j, expo in enumerate(expoList):
        warpedExposure = warper.warpExposure(
            destWcs = destExpo.getWcs(),
            srcExposure = expo,
            maxBBox = destExpo.getBBox())
        wn = "warpStatStack{}.fits".format(j)
        log.info(wn)
        #warpedExposure.writeFits(wn)
        j += 1
        maskedImage = warpedExposure.getMaskedImage()
        maskedImageList.append(maskedImage)
        weightList.append(1.0)

    coadd = afwMath.statisticsStack(maskedImageList, statsFlags, statsCtrl, weightList)
    #coadd.writeFits("coaddStatStack.fits")
    #coaddView <<= coadd
    destMaskedImage = destExpo.getMaskedImage()
    destMaskedImage <<= coadd
    return destExpo

def getSkyMapGoodPixelCopy(ctrCoord, width, height, filt, source, mapType, patchType):
    '''Merge multiple patches from a SkyMap into a single image.
    This is being left here in examples for reference purposes.
    This function should work in all SkyMap cases but is inefficient. It does not take
    advantage of the fact that all tracts in a SkyMap share the same WCS.
    It does take advantage of the fact that they have identical pixels where they overlap.
    @ ctrCoord: base coordinate of the box RA and Dec (minimum values, lower left corner)
    @ width: in Pixels
    @ height: in Pixels
    @ filter: valid filter for the data set such as 'i', 'r', 'g'
    @ source: source for Butler, such as "/lsst7/releaseW13EP"
    @ mapType: type of SkyMap, such as "deepCoadd_skyMap"
    @ patchType: patch type for butler to retrieve, such as "deepCoadd"
    width - in Pixels
    height - in Pixels
    filter - valid filter for the data set such as 'i', 'r', 'g'
    source - source for Butler, such as "/lsst7/releaseW13EP"
    mapType - type of SkyMap, such as "deepCoadd_skyMap"
    patchType - patch type for butler to retrieve, such as "deepCoadd"
    '''
    butler = lsst.daf.persistence.Butler(source)
    skyMap = butler.get(mapType)
    trInfo = skyMap.findTract(ctrCoord)
    destWcs = trInfo.getWcs()
    ctrCoordPix = destWcs.skyToPixel(ctrCoord)
    p2i = afwGeom.Point2I(ctrCoordPix)
    destBBox = afwGeom.Box2I(p2i, afwGeom.Extent2I(width, height))
    #rotMatrix = ???(rotation, scale)
    #destWcs = afw.image.makeWcs(ctrCoord, rot matrix, etc.) # pure tangent
    #destExposure = afwImage.ExposureF(destBBox, destWcs)

    # wcs needs floating point pixel position, so cast the integer bbox into a #double-precision bbox
    destCornerCoords = [destWcs.pixelToSky(pixPos) for pixPos in  afwGeom.Box2D(destBBox).getCorners()]
    tractPatchList = skyMap.findTractPatchList(destCornerCoords)
    expoList = []
    j = 0
    for tractPatch in tractPatchList:
        tractInfo = tractPatch[0]
        patchList = tractPatch[1]
        for patchInfo in patchList:
            tractId = tractInfo.getId()
            patchIndex = patchInfo.getIndex()
            pInd = ','.join(str(i) for i in patchIndex)
            patchExposure = butler.get(patchType, dataId={"filter": filt, "tract": tractId, "patch": pInd})
            #fname = "skyPatch{}.fits".format(j)
            #patchExposure.writeFits(fname)
            expoList.append(patchExposure)

    #config = CoaddConfig()
    warperConfig = afwMath.WarperConfig()
    warper = afwMath.Warper.fromConfig(warperConfig)

    stitchedExpo = stitchExposuresGoodPixelCopy(destWcs, destBBox, expoList, warper)
    #stitchedExpo.writeFits("skyOutGoodPixelCopy.fits")
    return stitchedExpo

def getSkyMapCoaddUtil(ctrCoord, width, height, filt, source, mapType, patchType):
    '''Merge multiple patches from a SkyMap into a single image.
    This is being left here in examples for reference purposes.
    This function works but is very inefficient.
    @ ctrCoord: base coordinate of the box RA and Dec (minimum values, lower left corner)
    @ width: in Pixels
    @ height: in Pixels
    @ filter: valid filter for the data set such as 'i', 'r', 'g'
    @ source: source for Butler, such as "/lsst7/releaseW13EP"
    @ mapType: type of SkyMap, such as "deepCoadd_skyMap"
    @ patchType: patch type for butler to retrieve, such as "deepCoadd"
    '''
    butler = lsst.daf.persistence.Butler(source)
    skyMap = butler.get(mapType)
    trInfo = skyMap.findTract(ctrCoord)
    destWcs = trInfo.getWcs()
    ctrCoordPix = destWcs.skyToPixel(ctrCoord)
    p2i = afwGeom.Point2I(ctrCoordPix)
    destBBox = afwGeom.Box2I(p2i, afwGeom.Extent2I(width, height))

    # wcs needs floating point pixel position, so cast the integer bbox into a #double-precision bbox
    destCornerCoords = [destWcs.pixelToSky(pixPos) for pixPos in  afwGeom.Box2D(destBBox).getCorners()] 
    #print "destCornerCoords", destCornerCoords
    tractPatchList = skyMap.findTractPatchList(destCornerCoords)
    expoList = []
    j = 0
    for tractPatch in tractPatchList:
        tractInfo = tractPatch[0]
        patchList = tractPatch[1]
        for patchInfo in patchList:
            tractId = tractInfo.getId()
            patchIndex = patchInfo.getIndex()
            pInd = ','.join(str(i) for i in patchIndex)
            patchExposure = butler.get(patchType, dataId={"filter": filt, "tract": tractId, "patch": pInd})
            #fname = "skyPatch{}.fits".format(j)
            #patchExposure.writeFits(fname)
            expoList.append(patchExposure)

    config = CoaddConfig()
    warperConfig = afwMath.WarperConfig()
    warper = afwMath.Warper.fromConfig(warperConfig)

    stitchedExpo = stitchExposures(destWcs, destBBox, expoList, config.coadd, warper)
    #stitchedExpo.writeFits("skyOutCoaddUtil.fits")
    return stitchedExpo

if __name__ == "__main__":
    #log.setLevel("", log.DEBUG)
    source = "/lsst7/releaseW13EP"
    mapType = "deepCoadd_skyMap"
    patchType = "deepCoadd"
    ctrCoord = afwCoord.Coord("1:01:00", "00:01:00", 2000.0)
    expo = getSkyMap(ctrCoord, 4000, 300, 'r', source, mapType, patchType)
    expo.writeFits("getSkyMap.fits")
    expo = getSkyMapGoodPixelCopy(ctrCoord, 4000, 300, 'r', source, mapType, patchType)
    expo.writeFits("getSkyMapGoodPixelCopy.fits")
    expo = getSkyMapCoaddUtil(ctrCoord, 4000, 300, 'r', source, mapType, patchType)
    expo.writeFits("getSkyMapCoaddUtil.fits")

