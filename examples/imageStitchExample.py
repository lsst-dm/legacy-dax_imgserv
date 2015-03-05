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
This is an example of stiching images together.

@author  John Gates, SLAC
"""

#input specifications are:

# See also obs_lsstSim policy/LsstSimMapper.paf to see all 
# available data products and the dataID keys used to obtain them.

import lsst.afw
import lsst.afw.geom as afwGeom
import lsst.afw.image as afwImage
import lsst.afw.math as afwMath
import lsst.coadd.utils as coaddUtils
import lsst.log as log
import lsst.pex.config as pexConfig

from lsst.imgserv.locateImage import dbOpen, W13DeepCoaddDb, W13RawDb
from lsst.imgserv.imageStitch import CoaddConfig, stitchExposures


def _getSubImg(imgSrc, box):
    subImg = imgSrc[box[0]:box[2], box[1]:box[3]].clone()
    return subImg

def stitchTestDeepCoadd():
    w13Db = dbOpen("~/.mysqlAuthLSST.lsst10", W13DeepCoaddDb)
    fullImg, metaFull = w13Db.getImageFullWithMetadata(19.36995, -0.3146)
    if fullImg == None:
        print "No image found"
        exit()
    # cut the image into quarters
    imgW = fullImg.getWidth()
    imgH = fullImg.getHeight()
    # ulImg - Upper Left image
    ulImg = _getSubImg(fullImg, [0, 0, imgW/2, imgH/2])
    urImg = _getSubImg(fullImg, [(imgW/2)+1, 0, imgW, imgH/2])
    llImg = _getSubImg(fullImg, [0, (imgH/2)+1, imgW/2, imgH])
    lrImg = _getSubImg(fullImg, [(imgW/2)+1, (imgH/2)+1, imgW, imgH])
    expoList = [ulImg, urImg, llImg, lrImg]
    fullImg.writeFits("full.fits")
    ulImg.writeFits("ul.fits")
    urImg.writeFits("ur.fits")
    llImg.writeFits("ll.fits")
    lrImg.writeFits("lr.fits")

    #fullWcs = lsst.afw.image.makeWcs(metaFull, False)
    fullWcs = fullImg.getWcs()
    fullImg.setWcs(fullWcs)
    srcExpo = ulImg
    srcWcs = fullWcs
    srcBBox = afwGeom.Box2I(afwGeom.Point2I(srcExpo.getX0(), srcExpo.getY0()),
                            afwGeom.Extent2I(srcExpo.getWidth(), srcExpo.getHeight()))
    destBBox = afwGeom.Box2I(afwGeom.Point2I(fullImg.getX0(), fullImg.getY0()),
                             afwGeom.Extent2I(imgW, imgH))
    destWcs = fullWcs
    #destExpo = afwImage.ExposureF(destBBox, destWcs)

    config = CoaddConfig()
    warperConfig = afwMath.WarperConfig()
    warper = afwMath.Warper.fromConfig(warperConfig)

    stitchedExpo = stitchExposures(destWcs, destBBox, expoList, config.coadd, warper)
    stitchedExpo.writeFits("stitched.fits")
    return

#def stitchExposures(destWcs, destBBox, expoList, configCoadd, warper):
#    ''' Return an exposure matching the destWcs and destBBox that is composed of
#    pixels from the exposures in expoList. The order of Exposures in expoList
#    matters as valid pixels in the final image will not be overwrtten.
#    pixels will be copied from expoList
#    All exposures need valid WCS.
#    '''
#    coadd = coaddUtils.Coadd.fromConfig(
#        bbox = destBBox,
#        wcs = destWcs,
#        config = configCoadd)
#    j = 0
#    for expo in expoList:
#        warpedExposure = warper.warpExposure(
#            destWcs = coadd.getWcs(),
#            srcExposure = expo,
#            maxBBox = coadd.getBBox())
#        log.info("warp{}".format(j))
#        warpedExposure.writeFits("warp{}.fits".format(j))
#        j += 1
#        coadd.addExposure(warpedExposure)
#
#    coaddExpo = coadd.getCoadd()
#    return coadd.getCoadd()

if __name__ == "__main__":
    log.setLevel("", log.DEBUG)
    stitchTestDeepCoadd()
