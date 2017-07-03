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
import lsst.afw.coord as afw_coord
import lsst.afw.geom as afw_geom
import lsst.afw.image as afw_image
import lsst.afw.math as afw_math

from .imageStitch import stitch_exposures_good_pixel_copy


def getSkyMap(center_coord, width, height, filter, units, source, map_type):
    """Merge multiple patches from a SkyMap into a single image.
    This function takes advantage of the fact that all tracts in a SkyMap share
    the same WCS and should have identical pixels where they overlap.
    @center_coord: base coordinate of the box RA and Dec (minimum values, lower left corner)
    @width: in Pixel
    @height: in Pixels
    @filter: valid filter for the data set such as 'i', 'r', 'g'
    @units: 'pixel' or 'arcsecond' (defaults to 'pixel')
    @source: source for Butler, such as "/lsst7/releaseW13EP"
    @map_type: type of SkyMap, such as "deepCoadd_skyMap"
    """
    # Get the basic SkyMap information
    butler = lsst.daf.persistence.Butler(source)
    sky_map = butler.get(map_type)
    dest_tract_info = sky_map.findTract(center_coord)
    dest_wcs = dest_tract_info.getWcs()
    # Determine target area.
    if units != 'arcsecond' and units != 'pixel':
        units = 'pixel'
    dest_bbox = _bbox_for_coords(dest_wcs, center_coord, width, height, units)
    dest_corner_coords = [dest_wcs.pixelToSky(pixPos)
                          for pixPos in afw_geom.Box2D(dest_bbox).getCorners()]
    # Collect patches of the SkyMap that are in the target region.
    # Create source exposures from the patches within each tract
    # as all patches from a tract share a WCS.
    exposure_list = []
    tract_patch_list = sky_map.findTractPatchList(dest_corner_coords)
    for j, tract_patch in enumerate(tract_patch_list):
        tract_info = tract_patch[0]
        patch_list = tract_patch[1]
        log.info("tract_info[{}]={}".format(j, tract_info))
        log.info("patch_list[{}]={}".format(j, patch_list))
        src_wcs = tract_info.getWcs()
        src_bbox = afw_geom.Box2I()
        for patch_info in patch_list:
            src_bbox.include(patch_info.getOuterBBox())
        src_exposure = afw_image.ExposureF(src_bbox, src_wcs)  # blank, so far
        exposure_list.append(src_exposure)

        # load srcExposures with patches
        tract_id = tract_info.getId()
        for patch_info in patch_list:
            patch_index = patch_info.getIndex()
            patch_index_str = ','.join(str(i) for i in patch_index)
            log.info("butler.get dataId=filter:{}, tract:{}, "
                     "patch:{}".format(filter, tract_id, patch_index_str))
            patch_exposure = butler.get("deepCoadd",
                                        dataId={"filter": filter,
                                                "tract": tract_id,
                                                "patch": patch_index_str})
            src_view = afw_image.ExposureF(src_exposure, patch_exposure.getBBox())
            src_view_img = src_view.getMaskedImage()
            patch_img = patch_exposure.getMaskedImage()
            src_view_img[:] = patch_img

    # Copy the pixels from the source exposures to the destination exposures.
    dest_exposure_list = []
    for j, src_exposure in enumerate(exposure_list):
        src_image = src_exposure.getMaskedImage()
        src_wcs = src_exposure.getWcs()
        if j == 0:
            expo_bbox = dest_bbox  # dest_bbox only correct for first image
        else:
            # Determine the correct BBox (in pixels) for the current src_wcs
            ll_corner = afw_geom.Point2I(src_wcs.skyToPixel(dest_corner_coords[0]))
            ur_corner = afw_geom.Point2I(src_wcs.skyToPixel(dest_corner_coords[2]))
            # Handle negative values for in expo_bbox.
            if ll_corner.getX() < 0:
                ll_corner.setX(0)
            if ll_corner.getY() < 0:
                ll_corner.setY(0)
            if ur_corner.getX() < 0:
                ur_corner.setX(0)
                log.warn("getSkyMap negative X for ur_corner")
            if ur_corner.getY() < 0:
                ur_corner.setY(0)
                log.warn("getSkyMap negative Y for ur_corner")
            expo_bbox = afw_geom.Box2I(ll_corner, ur_corner)
        log.info("j={} expo_bbox={} sBBox={}".format(j, expo_bbox, src_exposure.getBBox()))
        dest_exposure = afw_image.ExposureF(expo_bbox, src_wcs)
        dest_img = dest_exposure.getMaskedImage()
        begin_x = expo_bbox.getBeginX() - src_image.getX0()
        end_x = expo_bbox.getEndX() - src_image.getX0()
        begin_y = expo_bbox.getBeginY() - src_image.getY0()
        end_y = expo_bbox.getEndY() - src_image.getY0()

        new_width = src_exposure.getBBox().getEndX() - expo_bbox.getBeginX()
        new_height = src_exposure.getBBox().getEndY() - expo_bbox.getBeginY()

        # Do a final check to make sure that the we're not going past the end of src_image.
        src_img_len_x = src_image.getWidth()
        if end_x > src_img_len_x:
            new_width = src_img_len_x - begin_x
            end_x = src_img_len_x
        s_img_len_y = src_image.getHeight()
        if end_y > s_img_len_y:
            new_width = s_img_len_y - begin_y
            end_y = s_img_len_y

        log.debug("begin_x={} end_x={}".format(begin_x, end_x))
        log.debug("new_width{} = sBBox.EndX{} - sBBox.BeginX{}".format(
            new_width, src_exposure.getBBox().getEndX(), expo_bbox.getBeginX()))
        log.debug("begin_y={} end_y={}".format(begin_y, end_y))
        log.debug("new_height{} = sBBox.EndY{} - sBBox.BeginY{}".format(
            new_height, src_exposure.getBBox().getEndY(), expo_bbox.getBeginY()))
        dest_img[0:new_width, 0:new_height] = src_image[begin_x:end_x, begin_y:end_y]
        dest_exposure_list.append(dest_exposure)

    # If there's only one exposure in the list (and there usually is) just return it.
    if len(dest_exposure_list) == 1:
        return dest_exposure_list[0]

    # Need to stitch together the multiple destination exposures.
    log.debug("getSkyMap stitching together multiple destExposures")
    warper_config = afw_math.WarperConfig()
    warper = afw_math.Warper.fromConfig(warper_config)
    stitched_exposure = stitch_exposures_good_pixel_copy(dest_wcs, dest_bbox,
                                                         dest_exposure_list, warper)
    return stitched_exposure


def _bbox_for_coords(wcs, center_coord, width, height, units):
    """Returns a Box2I object representing the bounding box in pixels
    of the target region.
    @wcs: WCS object for the target region.
    @center_coord: RA and Dec coordinate for the center of the target region.
    @width: Width of the target region with units indicated by 'units' below.
    @height: Height of the target region with units indicated by 'units' below.
    @units: Units for height and width. 'pixel' or 'arcsecond'
    """
    if units == 'arcsecond':
        # center_coord center, RA and Dec with width and height in arcseconds
        width_half_a = afw_geom.Angle((width / 2.0), afw_geom.arcseconds)
        height_half_a = afw_geom.Angle((height / 2.0), afw_geom.arcseconds)
        min_ra = center_coord.getLongitude() - width_half_a
        min_dec = center_coord.getLatitude() - height_half_a
        max_ra = center_coord.getLongitude() + width_half_a
        max_dec = center_coord.getLatitude() + height_half_a
        ll_coord = afw_coord.Coord(min_ra, min_dec, center_coord.getEpoch())
        ll_coord_pix = wcs.skyToPixel(ll_coord)
        ur_coord = afw_coord.Coord(max_ra, max_dec, center_coord.getEpoch())
        ur_coord_pix = wcs.skyToPixel(ur_coord)
        p2i_min = afw_geom.Point2I(ll_coord_pix)
        p2i_max = afw_geom.Point2I(ur_coord_pix)
        bbox = afw_geom.Box2I(p2i_min, p2i_max)
    elif units == 'pixel':
        # center_coord center, RA and Dec with width and height in pixels
        ctr_coord_pix = wcs.skyToPixel(center_coord)
        min_ra_pix = int(ctr_coord_pix.getX() - width//2)
        min_dec_pix = int(ctr_coord_pix.getY() - height//2)
        p2i = afw_geom.Point2I(min_ra_pix, min_dec_pix)
        bbox = afw_geom.Box2I(p2i, afw_geom.Extent2I(width, height))
    else:
        raise Exception("invalid units {}".format(units))
    return bbox
