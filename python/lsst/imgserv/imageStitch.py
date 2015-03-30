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
    pixels from the exposures in expoList.
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
        warpedExposure.writeFits("warp{}.fits".format(j))
        j += 1
        coadd.addExposure(warpedExposure)

    return coadd.getCoadd()
