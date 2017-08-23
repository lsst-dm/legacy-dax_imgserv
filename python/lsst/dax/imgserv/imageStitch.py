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

import lsst.afw.image as afw_image
import lsst.coadd.utils as coadd_utils
import lsst.pex.config as pex_config


class CoaddConfig(pex_config.Config):
    saveDebugImages = pex_config.Field(
        doc="Save intermediate images?",
        dtype=bool,
        default=False,
    )
    bboxMin = pex_config.ListField(
        doc="Lower left corner of bounding box used to subframe to all input images",
        dtype=int,
        default=(0, 0),
        length=2,
    )
    bboxSize = pex_config.ListField(
        doc="Size of bounding box used to subframe all input images; 0 0 for full input images",
        dtype=int,
        default=(0, 0),
        length=2,
    )
    coaddZeroPoint = pex_config.Field(
        dtype=float,
        doc="Photometric zero point of coadd (mag).",
        default=27.0,
    )
    coadd = pex_config.ConfigField(dtype=coadd_utils.Coadd.ConfigClass, doc="")


def stitch_exposures(dest_wcs, dest_bbox, expo_list, coadd_config, warper):
    """Return an exposure matching the dest_wcs and dest_bbox that is composed of
    pixels from the exposures in expo_list. Uses coadd_utils.Coadd.
    dest_wcs     - WCS object for the destination exposure.
    dest_bbox    - Bounding box for the destination exposure.
    expo_list    - List of exposures to combine to form dextination exposure.
    coadd_config - configuration for Coadd
    warper      - Warper to use when warping images.
    All exposures need valid WCS.
    """
    coadd = coadd_utils.Coadd.fromConfig(
        bbox=dest_bbox,
        wcs=dest_wcs,
        config=coadd_config)
    for j, expo in enumerate(expo_list):
        warped_exposure = warper.warpExposure(
            destWcs=coadd.getWcs(),
            srcExposure=expo,
            maxBBox=coadd.getBBox())
        log.info("warp{}".format(j))
        j += 1
        coadd.addExposure(warped_exposure)

    return coadd.getCoadd()


def stitch_exposures_good_pixel_copy(dest_wcs, dest_bbox, expo_list, warper,
                                     bad_pixel_mask=None):
    """ Return an exposure matching the dest_wcs and dest_bbox that is composed of
    pixels from the exposures in expo_list. Uses coadd_utils.goodPixelCopy
    @ dest_wcs: WCS object for the destination exposure.
    @ dest_bbox: Bounding box for the destination exposure.
    @ expo_list: List of exposures to combine to form dextination exposure.
    @ warper: Warper to use when warping images.
    @ bad_pixel_mask: mask for pixels that should not be copied.
    All exposures need valid WCS.
    """
    if bad_pixel_mask is None:
        bad_pixel_mask = afw_image.MaskU.getPlaneBitMask(["EDGE"])
    dest_expo = afw_image.ExposureF(dest_bbox, dest_wcs)
    for j, expo in enumerate(expo_list):
        warped_exposure = warper.warpExposure(
            destWcs=dest_expo.getWcs(),
            srcExposure=expo,
            maxBBox=dest_expo.getBBox())
        src_masked_image = warped_exposure.getMaskedImage()
        dest_masked_image = dest_expo.getMaskedImage()
        coadd_utils.copyGoodPixels(dest_masked_image, src_masked_image, bad_pixel_mask)
    return dest_expo
