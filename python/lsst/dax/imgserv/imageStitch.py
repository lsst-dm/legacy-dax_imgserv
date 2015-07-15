#!/usr/bin/python

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
import lsst.afw.geom as afwGeom
import lsst.afw.image as afwImage
import lsst.afw.math as afwMath
import lsst.coadd.utils as coaddUtils
import lsst.pex.config as pexConfig

import matplotlib.pyplot as plt

class CoaddConfig(pexConfig.Config):
    saveDebugImages = pexConfig.Field(
        doc = "Save intermediate images?",
        dtype = bool,
        default = False,
    )
    bboxMin = pexConfig.ListField(
        doc = "Lower left corner of bounding box used to subframe to all input images",
        dtype = int,
        default = (0, 0),
        length = 2,
    )
    bboxSize = pexConfig.ListField(
        doc = "Size of bounding box used to subframe all input images; 0 0 for full input images",
        dtype = int,
        default = (0, 0),
        length = 2,
    )
    coaddZeroPoint = pexConfig.Field(
        dtype = float,
        doc = "Photometric zero point of coadd (mag).",
        default = 27.0,
    )
    coadd = pexConfig.ConfigField(dtype = coaddUtils.Coadd.ConfigClass, doc = "")
    #warp = pexConfig.ConfigField(dtype = afwMath.Warper.ConfigClass, doc = "")

def stitchExposures(destWcs, destBBox, expoList, configCoadd, warper):
    ''' Return an exposure matching the destWcs and destBBox that is composed of
    pixels from the exposures in expoList. Uses coadd_utils.Coadd.
    destWcs     - WCS object for the destination exposure.
    destBBox    - Bounding box for the destination exposure.
    expoList    - List of exposures to combine to form dextination exposure.
    configCoadd - configuration for Coadd
    warper      - Warper to use when warping images.
    All exposures need valid WCS.
    '''
    coadd = coaddUtils.Coadd.fromConfig(
        bbox = destBBox,
        wcs = destWcs,
        config = configCoadd)
    for j, expo in enumerate(expoList):
        warpedExposure = warper.warpExposure(
            destWcs = coadd.getWcs(),
            srcExposure = expo,
            maxBBox = coadd.getBBox())
        log.info("warp{}".format(j))
        #warpedExposure.writeFits("warp{}.fits".format(j))
        j += 1
        coadd.addExposure(warpedExposure)

    return coadd.getCoadd()

def stitchExposuresGoodPixelCopy(destWcs, destBBox, expoList, warper,
    badPixelMask = afwImage.MaskU.getPlaneBitMask(["EDGE"])):
    ''' Return an exposure matching the destWcs and destBBox that is composed of
    pixels from the exposures in expoList. Uses coadd_utils.goodPixelCopy
    @ destWcs: WCS object for the destination exposure.
    @ destBBox: Bounding box for the destination exposure.
    @ expoList: List of exposures to combine to form dextination exposure.
    @ warper: Warper to use when warping images.
    @ badPixelMask: mask for pixels that should not be copied.
    All exposures need valid WCS.
    '''
    destExpo = afwImage.ExposureF(destBBox, destWcs)
    for j, expo in enumerate(expoList):
        warpedExposure = warper.warpExposure(
            destWcs = destExpo.getWcs(),
            srcExposure = expo,
            maxBBox = destExpo.getBBox())
        srcMaskedImage = warpedExposure.getMaskedImage()
        destMaskedImage = destExpo.getMaskedImage()
        coaddUtils.copyGoodPixels(destMaskedImage, srcMaskedImage, badPixelMask)
    return destExpo


def strExpoCornersRaDec(expo):
    wcs = expo.getWcs()
    x0 = expo.getX0()
    y0 = expo.getY0()
    w = expo.getWidth()
    h = expo.getHeight()
    llCorner = wcs.pixelToSky(x0, y0)
    urCorner = wcs.pixelToSky(x0+w, y0+h)
    s = "llCorner={} urCorner={}".format(llCorner, urCorner)
    return s
