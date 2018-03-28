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
This library module is used to locate and retrieve variolus image types and
cutout dimensions, via the appropriate Butler object passed in.

@author: John Gates, SLAC
@author: Brian Van Klaveren, SLAC
@author: Kenny Lo, SLAC

"""

import math

import lsst.afw
import lsst.afw.coord as afw_coord
import lsst.afw.geom as afw_geom
import lsst.afw.image as afw_image
import lsst.log as log

from .skymapImage import SkymapImage

class ImageGetter_v1:
    """Provide operations to retrieve images including cutouts from the specified
    image repository through the passed-in butler and metaserv.

    """

    def __init__(self, butlerget, metaservget, logger):
        """ Instantiate ImageGetter object with butler, butler configuration,
        and connection for image metadata.

        Parameters
        ----------
        butlerget : locateImage.ButlerGet
            the butler instance and config info.
        metaservget : locateImage.MetaservGet
            provides access to image metadata.
        logger : log
            the logger to be used.
        """
        self._log = logger
        self._butler = butlerget.butler
        self._butler_keys = sorted(butlerget.butler_keys)
        self._imagedataset_type = butlerget.butler_policy
        self._metaservget = metaservget

    def full_nearest(self, x, y, unit, filt):
        """Returns image containing center(x,y) of unit and filter.

        Parameters
        ----------
        x : float
            x-coordinate, or Ra if Equatorial
        y : float
            y-coordinate, or Dec if Equatorial
        unit : string
            [ 'px', 'pix", 'pixel', 'pixels', 'arcsec', 'arcmin', 'deg' ]
        filt : string (optional)
            ['u', 'g', 'r', 'i', 'z', 'y']

        Returns
        -------
        lsst.afw.Image or None

        """
        # assume (x,y) is Equatorial coordinates here
        ra = x
        dec = y
        filtername = filt
        qresult = self._metaservget.nearest_image_containing(ra, dec,
                filtername)
        if qresult == []:
            # not found
            return None
        else:
            data_id = self._data_id_from_qr(qresult)
            image = self._image_from_butler(data_id)
            return image

    def full_from_data_id_by_run(self, run, camcol, field, filt):
        """Returns image from specified data id (run, camcol, field, filter).

        Parameters
        ----------
        run : int
        camcol : int
        field : int
        filt : string
            ['u', 'g', 'r', 'i', 'z', 'y']

        Returns
        -------
        lsst.afw.Image or None

        """
        image = self._butler.get(self._imagedataset_type, run=run,
                camcol=camcol, field=field, filter=filt)
        return image

    def full_from_data_id_by_tract(self, tract, patch_x, patch_y, filt):
        """Returns image from specified data id (tract, patch<x,y>, filter).

        Parameters
        ----------
        tract : int
        patch_x : int
        patch_y : int
        filt : string
            ['u', 'g', 'r', 'i', 'z', 'y']

        Returns
        -------
        lsst.afw.Image or None

        """
        patch = ",".join((str(patch_x), str(patch_y)))
        image = self._butler.get(self._imagedataset_type, tract=tract,
                patch=patch, filter=filt)
        return image

    def full_from_science_id(self, science_id):
        """Returns image from the science id.

        Parameters
        ----------
        science_id : int

        Returns
        -------
        lsst.afw.Image or None

        """
        data_id = self.data_id_from_science_id(science_id)
        if data_id:
            image = self._image_from_butler(data_id)
            return image

    def cutout_from_nearest(self, center_x, center_y, center_unit, size_x,
            size_y, size_unit, filt):
        """Returns the cutout image at center (x,y) of unit and size.

        Parameters
        ----------
        cetner_x : float
        cetner_y : float
        center_unit : string
            ['px', 'pix', 'pixel', 'pixels', 'arcsec', 'arcmin', 'deg']
        size_x : float
        size_y : float
        size_unit : string
        filt: string

        Returns
        -------
        lsst.afw.Image or None

        """
        ra, dec = center_x, center_y
        filtername = filt
        width, height = size_x, size_y
        qresult = self._metaservget.nearest_image_containing(ra, dec, filtername)
        if qresult == []:
            # not found
            return None
        else:
            data_id = self._data_id_from_qr(qresult)
            image = self._imagecutout_by_data_id(ra, dec, width, height,
                    data_id, size_unit)
            return image

    def cutout_from_data_id_by_run(self, run, camcol, field, filt, center_x,
            center_y, center_unit, size_x, size_y, size_unit):
        """Returns cutout image from data id (run, camcol, field, filtername)
        of specified center.

        Parameters
        ----------
        run : int
        camcol : int
        field : int
        filt : string
        center_x : float
        center_y : float
        cetner_unit : string
            ['px', 'pix', 'pixel', 'pixels', 'arcsec', 'arcmin', 'deg']
        size_x : float
        size_y : float
        size_unit : string

        Returns
        -------
        lsst.afw.Image or None

        """
        ra, dec = center_x, center_y
        width, height = size_x, size_y
        data_id = {"run": run, "camcol": camcol, "field": field, "filter": filt}
        image = self._imagecutout_by_data_id(ra, dec, width, height, data_id,
                size_unit)
        return image

    def cutout_from_data_id_by_tract(self, tract, patch_x, patch_y, filt, center_x,
            center_y, center_unit, size_x, size_y, size_unit):
        """Returns cutout image from data id (tract, patch<x,y>, filt)
        of specified center.

        Parameters
        ----------
        tract : int
        patch_x : int
        pactch_y : int
        filt : string
        center_x : float
        cetner_y : float
        center_unit : string
            ['px', 'pix', 'pixel, 'pixels', 'arcsec', 'arcmin', 'deg']
        size_x : float
        size_y : float
        size_unit : string

        Returns
        -------
        lsst.afw.Image or None

        """
        ra, dec = center_x, center_y
        width, height = size_x, size_y
        patch = ",".join((str(patch_x), str(patch_y)))
        data_id = {"tract": tract, "patch": patch, "filter": filt}
        image = self._imagecutout_by_data_id(ra, dec, width, height, data_id,
                size_unit)
        return image

    def cutout_from_science_id(self, science_id, center_x, center_y,
                center_unit, size_x, size_y, size_unit):
        """
        Parameters
        ----------
        science_id : int
        center_x : float
        center_y : float
        center_unit : string
            [ 'px', 'pix', 'pixel', 'pixels', 'arcsec', 'arcmin', 'deg' ]
        size_x : float
        size_y : float
        size_unit : string
            [ 'px', 'pix', 'pixel', 'pixels', 'arcsec', 'arcmin', 'deg' ]

        Returns
        -------
        lsst.afw.Image or None

        """
        ra, dec = center_x, center_y
        width, height = size_x, size_y
        # Get the corresponding image(data) id from the butler
        data_id = self.data_id_from_science_id(science_id)
        if data_id:
            # make id compatible with qResult type via custom wrapping
            image = self._imagecutout_by_data_id(ra, dec, width, height,
                    data_id, size_unit)
            return image

    def cutout_from_skymap_id(self, skymap_id, filt, center_x, center_y,
                center_unit, size_x, size_y, size_unit):
        """
        Parameters
        ----------
        skymap_id : string
        filt : filter
        center_x : float
        center_y : float
        center_unit : string
            [ 'px', 'pix', 'pixel', 'pixels', 'arcsec', 'arcmin', 'deg' ]
        size_x : float
        size_y : float
        size_unit : string
            [ 'px', 'pix', 'pixel', 'pixels', 'arcsec', 'arcmin', 'deg' ]

        Returns
        -------
        lsst.afw.Image or None

        """
        ra, dec = center_x, center_y
        width, height = size_x, size_y
        skymap = SkymapImage(self._butler, skymap_id, self._log)
        ra_angle = afw_geom.Angle(ra, afw_geom.degrees)
        dec_angle = afw_geom.Angle(dec, afw_geom.degrees)
        center_coord = afw_coord.Coord(ra_angle, dec_angle, 2000.0)
        image = skymap.get(center_coord, width, height, filt, center_unit)
        return image

    def data_id_from_science_id(self, science_id):
        """
        Parameters
        ----------
        science_id: int

        Returns
        -------
        dict - list of ids derived from scienceId.
        The ids match the ids in _butler_keys and valid is false
        if at least one of the ids is missing.

        """
        data_id = {}
        science_id = int(science_id)
        if self._butler_keys == sorted(["run", "camcol", "field", "filter"]):
            possible_fields = {
                "field": science_id % 10000,
                "camcol": (science_id // 10000) % 10,
                "filter": "ugriz"[(science_id // 100000) % 10],
                "run": science_id // 1000000,
            }
            self._log.debug("data_id_from_science_id {}".format(
                possible_fields))
            for key in self._butler_keys:
                value = possible_fields[key]
                data_id[key] = value
            self._log.debug("dataID={}".format(data_id))
        elif self._butler_keys == sorted(["tract", "patch", "filter"]):
            patch_y = (science_id // 8) % (2 ** 13)
            patch_x = (science_id // (2**16)) % (2 ** 13)
            possible_fields = {
                "filter": "ugriz"[science_id % 8],
                "tract": science_id // (2 ** 29),
                "patch": "%d,%d" % (patch_x, patch_y)
            }
            self._log.debug("data_id_from_science_id {}".format(
                possible_fields))
            for key in self._butler_keys:
                value = possible_fields[key]
                data_id[key] = value
            self._log.debug("dataID={}".format(data_id))
        return data_id

    def scienceid_from_dataid(self, data_id):
        """Compose and return the science id corresponding to the data id input.

        Parameters
        ----------
        data_id(dict):  the data id as input.

        Returns
        -------
        int - the science id.

        """
        science_id = self._butler.get('ccdExposureId', dataId=data_id)
        return science_id

    def _imagecutout_by_data_id(self, ra, dec, width, height, data_id,
            unit="arcsec"):
        # check to see if qresults is empty
        if data_id is None:
            return None
        # Return an image by data ID through the butler.
        image = self._image_from_butler(data_id)
        if image is None:
            # @todo html error handling see DM-1980
            return None
        # Get the metadata for the source image.
        metadata = self._metadata_from_data_id(data_id)
        cutout = self._apply_cutout(image, metadata, ra, dec, width, height,
                data_id, unit)
        return cutout

    def _apply_cutout(self, src_img, metadata, ra, dec, width, height, data_id,
            unit="arcsec"):
        """Return an image centered on ra and dec (in degrees) with dimensions
        height and width (in arcseconds by default).

        - Determine approximate pixels per arcsec in the image by
             calculating the length of line from the upper right corner of
             the image to the lower left corner in pixels and arcsecs.
             (This will fail at or very near the pole.)
        - Use that to define a box for the cutout.
        - Trim the box so it is entirely within the source image.

        Parameters
        ----------
        ra, dec :  float
            ra, dec in degrees.
        height, width : float, float
            Height and width are in arcsecs.

        Returns
        -------
        lsst.afw.Image

        """
        self._log.debug("apply_cutout %f %f %f %f", ra, dec, width, height)
        # False: do not remove FITS keywords from metadata
        wcs = None
        if isinstance(src_img, afw_image.ExposureF):
            wcs = src_img.getWcs()
        if wcs is None and metadata:
            # try to use the metadata
            wcs = afw_geom.makeSkyWcs(metadata, strip=False)
        if wcs is None:
            raise Exception("WCS missing in source image")
        radec = afw_coord.IcrsCoord(ra*afw_geom.degrees, dec*afw_geom.degrees)
        xy_wcs = wcs.skyToPixel(radec)
        xy_center_x = xy_wcs.getX()
        xy_center_y = xy_wcs.getY()
        self._log.debug("ra=%f dec=%f xy_center=(%f,%f)",
                ra, dec, xy_center_x, xy_center_y)
        if unit == 'pixel':
            cutout = self._cutout_from_src(src_img,
                    xy_center_x, xy_center_y,
                    width, height, wcs)
            return cutout
        img_w, img_h = src_img.getWidth(), src_img.getHeight()
        self._log.debug("src_img_w=%d src_img_h=%d", img_w, img_h)
        # Determine approximate pixels per arcsec - find image corners in RA, Dec
        # and compare that distance with the number of pixels.
        radec_ul = wcs.pixelToSky(afw_geom.Point2D(0, 0))
        radec_lr = wcs.pixelToSky(afw_geom.Point2D(img_w - 1, img_h - 1))
        self._log.debug("radec_ul 0=%f 1=%f",
                        radec_ul[0].asDegrees(), radec_ul[1].asDegrees())
        self._log.debug("radec_lr 0=%f 1=%f",
                        radec_lr[0].asDegrees(), radec_lr[1].asDegrees())
        # length of a line from upper left (UL) to lower right (LR)
        dec_dist = radec_ul[1].asArcseconds() - radec_lr[1].asArcseconds()
        ra_lr = self._keep_within_180(radec_ul[0].asDegrees(),
                radec_lr[0].asDegrees())
        ra_lr *= 3600.0  # convert degrees to arcsecs
        # Correct distance in RA for the declination
        cos_dec = math.cos(dec * afw_geom.degrees)
        ra_dist = cos_dec * (radec_ul[0].asArcseconds() - ra_lr)
        radec_dist = math.sqrt(math.pow(dec_dist, 2.0) + math.pow(ra_dist, 2.0))
        self._log.debug("radec_dist=%f", radec_dist)
        pixel_dist = math.sqrt(math.pow(img_w, 2.0) + math.pow(img_h, 2.0))
        pixel_per_arcsec = pixel_dist / radec_dist
        self._log.debug("pixel_per_arcsec=%f", pixel_per_arcsec)
        # Need Upper Left corner and dimensions for Box2I
        pix_w = width * pixel_per_arcsec
        pix_h = height * pixel_per_arcsec
        self._log.debug("ra=%f dec=%f xy_wcs=(%f,%f) xyCenter=(%f,%f)",
                ra, dec, xy_wcs.getX(), xy_wcs.getY(), xy_center_x, xy_center_y)
        cutout = self._cutout_from_src(src_img, xy_center_x, xy_center_y,
                pix_w, pix_h, wcs)
        return cutout

    def _metadata_from_data_id(self, data_id):
        # Return the metadata for the query results in qResults and a butler.
        if self._butler_keys == sorted(["run", "camcol", "field", "filter"]):
            metadata = self._butler.get(self._imagedataset_md(),
                    run=data_id["run"],
                    camcol=data_id["camcol"],
                    field=data_id["field"],
                    filter=data_id["filter"])
        elif self._butler_keys == sorted(["tract", "patch", "filter"]):
            metadata = self._butler.get(self._imagedataset_md(),
                    tract=data_id["tract"],
                    patch=data_id["patch"],
                    filter=data_id["filter"])
        else:
            # no metadata found for the specified data id
            metadata = None
        return metadata

    def _image_from_butler(self, data_id):
        # Retrieve the image through the Butler using data id.
        self._log.debug("_image_from_butler data_id:{}".format(data_id))
        if self._butler_keys == sorted(["run", "camcol", "field", "filter"]):
            run = data_id.get("run")
            camcol = data_id.get("camcol")
            field = data_id.get("field")
            filtername = data_id.get("filter")
            log.debug("_image_from_butler run={} camcol={} field={} "
                      "filter={}".format(run, camcol, field, filtername))
            image = self._butler.get(self._imagedataset_type, run=run,
                                   camcol=camcol, field=field, filter=filtername)
            return image
        elif self._butler_keys == sorted(["tract", "patch", "filter"]):
            tract = data_id.get("tract")
            filtername = data_id.get("filter")
            patch = data_id.get("patch")
            self._log.debug("_image_from_butler tract={} patch={} "
                            "filtername={}".format(tract, patch, filtername))
            image = self._butler.get(self._imagedataset_type, tract=tract,
                                   patch=patch, filter=filtername)
            return image

    def _imagedataset_md(self):
        # Return the butler policy name to retrieve metadata
        return self._imagedataset_type + "_md"

    def _cutout_from_src(self, src_image, xy_center_x, xy_center_y, width,
            height, wcs):
        # Returns an image cutout from the source image.
        # srcImage - Source image.
        # xy_center - The center of region to cutout in pixels.
        # width - The width in pixels.
        # height - The height in pixels.
        # height and width to be trimmed if they go past the edge of source image.
        # First, center the cutout image.
        pix_ulx = int(xy_center_x - width / 2.0)
        pix_uly = int(xy_center_y - height / 2.0)
        xy_center = afw_geom.Point2I(pix_ulx, pix_uly)
        log.debug("xy_center={}".format(xy_center))
        src_box = src_image.getBBox()
        # assuming both src_box and xy_center to be in Box2I
        co_box = afw_geom.Box2I(xy_center,
                afw_geom.Extent2I(int(width), int(height)))
        if co_box.overlaps(src_box):
            co_box.clip(src_box)
        else:
            self._log.debug("cutout image wanted is OUTSIDE source image -> None")
            return None
        if isinstance(src_image, afw_image.ExposureF):
            self._log.debug("co_box pix_ulx={} pix_end_x={} pix_uly={} pix_end_y={}"
                  .format(pix_ulx, pix_ulx + width, pix_uly, pix_uly + height))
            # img will keep wcs from source image
            image = afw_image.ExposureF(src_image, co_box)
        else:
            # hack for non-ExposureF, e.g. raw (DecoratedImage)
            pix_ulx = co_box.getBeginX() - src_image.getX0()
            pix_end_x = co_box.getEndX() - src_image.getX0()
            pix_uly = co_box.getBeginY() - src_image.getY0()
            pix_end_y = co_box.getEndY() - src_image.getY0()
            self._log.debug("co_box pix_ulx={} pix_end_x={} pix_uly={} \
                    pix_end_y={}".format(pix_ulx, pix_end_x, pix_uly, pix_end_y))
            image = src_image[pix_ulx:pix_end_x, pix_uly:pix_end_y].clone()
            # add back wcs for image types, e.g. raw
            if wcs:
                d_image = afw_image.DecoratedImageU(image)
                _wcs = wcs.getFitsMetadata(precise=False)
                d_image.setMetadata(_wcs)
                return d_image
        return image

    def _data_id_from_qr(self, qresults):
        # identify and fetch the data ID from 1 of 2 defined sets
        ln = qresults[0]
        # first try run, camcol, field, filter keys
        if self._butler_keys == sorted(["run", "camcol", "field", "filter"]):
            run, camcol, field, filtername = ln[2:6]
            data_id = {"run": run, "camcol": camcol, "field": field,
                    "filter": filtername}
            return data_id
        # if no match, then try tract, patch, filter keys
        if self._butler_keys == sorted(["tract", "patch", "filter"]):
            tract, patch, filtername = ln[2:5]
            data_id = {"tract": tract, "patch": patch, "filter": filtername}
            return data_id

    def _keep_within_180(self, target, val):
        # Return a value that is equivalent to val on circle
        # within 180 degrees of target.
        while val > (target + 180.0):
            val -= 360.0
        while val < (target - 180.0):
            val += 360.0
        return val
