# This file is part of dax_imgserv.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
This library module is used to locate and retrieve variolus image types and
cutout dimensions, via the appropriate Butler object passed in.

@author: John Gates, SLAC
@author: Brian Van Klaveren, SLAC
@author: Kenny Lo, SLAC

"""
import lsst.afw.geom as afwGeom
import lsst.afw.image as afwImage
from lsst.afw.geom import SpanSet, Stencil

import lsst.log as log

from .skymapImage import SkymapImage


class ImageGetter:
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

    def full_nearest(self, ra, dec, filt):
        """Returns image containing center(x,y) of unit and filter.

        Parameters
        ----------
        ra : `float`
            x-coordinate, or Ra if Equatorial
        dec : `float`
            y-coordinate, or Dec if Equatorial
        filt(optional): `str`
            ['u', 'g', 'r', 'i', 'z', 'y']

        Returns
        -------
        image : `afwImage.Exposure`

        """
        q_result = self._metaservget.nearest_image_containing(ra, dec, filt)
        if not q_result:
            # not found
            raise Exception("Image not found")
        else:
            data_id = self._data_id_from_qr(q_result)
            image = self._image_from_butler(data_id)
            return image

    def full_from_data_id_by_run(self, run, camcol, field, filt):
        """Returns image from specified data id (run, camcol, field, filter).

        Parameters
        ----------
        run : `int`
        camcol : `int`
        field : `int`
        filt : `str`
            ['u', 'g', 'r', 'i', 'z', 'y']

        Returns
        -------
        image: `afwImage.Exposure`

        """
        image = self._butler.get(self._imagedataset_type, run=run,
                                 camcol=camcol, field=field, filter=filt)
        return image

    def full_from_data_id_by_tract(self, tract, patch_x, patch_y, filt):
        """Returns image from specified data id (tract, patch<x,y>, filter).

        Parameters
        ----------
        tract : `int`
        patch_x : `int`
        patch_y : `int`
        filt : `str`
            ['u', 'g', 'r', 'i', 'z', 'y']

        Returns
        -------
        image : `afwImage.Exposure`

        """
        patch = ",".join((str(patch_x), str(patch_y)))
        image = self._butler.get(self._imagedataset_type, tract=tract,
                                 patch=patch, filter=filt)
        return image

    def full_from_ccd_exp_id(self, ccd_exp_id):
        """Returns image from the science id.

        Parameters
        ----------
        ccd_exp_id : `int`

        Returns
        -------
        image: `afwImage.Expsoure`

        """
        data_id = self.data_id_from_ccd_exp_id(ccd_exp_id)
        if data_id:
            image = self._image_from_butler(data_id)
            return image
        else:
            raise Exception("data id not found")

    def cutout_from_nearest(self, ra, dec, width, height, unit, filt):
        """Returns the cutout image at center (x,y) of unit and size.

        Parameters
        ----------
        ra : `float`
        dec : `float`
            in degrees.
        width : `float`
        height  : `float`
        unit : `str`
            [ 'px', 'pix', 'pixel', 'pixels', 'arcsec' ]
        filt: `str`

        Returns
        -------
        cutout: `afwImage.Exposure`

        """
        q_result = self._metaservget.nearest_image_containing(ra, dec, filt)
        if not q_result:
            # not found
            return None
        else:
            data_id = self._data_id_from_qr(q_result)
            cutout = self._cutout_by_data_id(data_id, ra, dec, width, height,
                                             unit)
            return cutout

    def cutout_from_data_id_by_run(self, run, camcol, field, filt, ra, dec,
                                   width, height, unit):
        """Returns cutout image from data id (run, camcol, field, filtername)
        of specified center.

        Parameters
        ----------
        run : `int`
        camcol : `int`
        field : `int`
        filt : `str`
        ra : `float`
        dec: `float`
        width : `float`
        height : `float`
        unit : `str`
            [ 'px', 'pix', 'pixel', 'pixels', 'arcsec' ]
        Returns
        -------
        cutout: `afwImage.Exposure`

        """
        data_id = {"run": run, "camcol": camcol, "field": field, "filter": filt}
        cutout = self._cutout_by_data_id(data_id, ra, dec, width, height,
                                         unit)
        return cutout

    def cutout_from_data_id_by_tract(self, tract, patch_x, patch_y, filt,
                                     ra, dec, width, height,
                                     unit):
        """Returns cutout image from data id (tract, patch<x,y>, filt)
        of specified center.

        Parameters
        ----------
        tract : `int`
        patch_x : `int`
        pactch_y : `int`
        filt : `str`
        ra: `float`
        dec : `float`
            in degrees.
        width : `float`
        height: `float`
        unit : `str`

        Returns
        -------
        cutout: `afwImage.Exposure`

        """
        patch = ",".join((str(patch_x), str(patch_y)))
        data_id = {"tract": tract, "patch": patch, "filter": filt}
        cutout = self._cutout_by_data_id(data_id, ra, dec, width, height, unit)
        return cutout

    def cutout_from_ccd_exp_id(self, ccd_exp_id, ra, dec, width, height,
                               unit):
        """
        Parameters
        ----------
        ccd_exp_id : `int`
        ra : `float`
        dec: `float`
            in degrees.
        width : `float`
        height : `float`
        unit : `str`
            [ `pixel', 'pixels', 'arcsec' ]

        Returns
        -------
        cutout: `afwImage.Exposure`

        """
        # Get the corresponding image(data) id from the butler
        data_id = self.data_id_from_ccd_exp_id(ccd_exp_id)
        if data_id:
            # make id compatible with qResult type via custom wrapping
            cutout = self._cutout_by_data_id(data_id, ra, dec, width, height,
                                             unit)
            return cutout

    def cutout_from_skymap_id(self, skymap_id, filt, ra, dec, width, height,
                              unit):
        """
        Parameters
        ----------
        skymap_id : `str`
        filt : filter
        ra : `float`
        dec : `float`
        width : `float`
        height : `float`
        unit : `str`
            [ 'px', 'pix', 'pixel', 'pixels', 'arcsec']

        Returns
        -------
        cutout: `afwImage.Exposure`

        """
        skymap = SkymapImage(self._butler, skymap_id, self._log)
        center_coord = afwGeom.SpherePoint(ra, dec, afwGeom.degrees)
        cutout = skymap.get(center_coord, width, height, filt, unit)
        return cutout

    def data_id_from_ccd_exp_id(self, ccd_exp_id):
        """ The ids match the ids in _butler_keys and valid is false
        if at least one of the ids is missing.

        Parameters
        ----------
        ccd_exp_id: `int`

        Returns
        -------
        data_id : `dict`
            the list of ids derived from scienceId.

        """
        data_id = {}
        ccd_exp_id = int(ccd_exp_id)
        if self._butler_keys == sorted(["run", "camcol", "field", "filter"]):
            possible_fields = {
                "field": ccd_exp_id % 10000,
                "camcol": (ccd_exp_id // 10000) % 10,
                "filter": "ugriz"[(ccd_exp_id // 100000) % 10],
                "run": ccd_exp_id // 1000000,
            }
            self._log.debug("data_id_from_ccd_exp_id {}".format(
                possible_fields))
            for key in self._butler_keys:
                value = possible_fields[key]
                data_id[key] = value
            self._log.debug("dataID={}".format(data_id))
        elif self._butler_keys == sorted(["tract", "patch", "filter"]):
            patch_y = (ccd_exp_id // 8) % (2 ** 13)
            patch_x = (ccd_exp_id // (2 ** 16)) % (2 ** 13)
            possible_fields = {
                "filter": "ugriz"[ccd_exp_id % 8],
                "tract": ccd_exp_id // (2 ** 29),
                "patch": "%d,%d" % (patch_x, patch_y)
            }
            self._log.debug("data_id_from_ccd_exp_id {}".format(
                possible_fields))
            for key in self._butler_keys:
                value = possible_fields[key]
                data_id[key] = value
            self._log.debug("dataID={}".format(data_id))
        return data_id

    def ccd_exp_id_from_data_id(self, data_id):
        """Compose and return the science id corresponding to the data id input.

        Parameters
        ----------
        data_id: `dict`
            the data id as input.

        Returns
        -------
        ccd_exp_id : int
            the CCD Exposure id.

        """
        ccd_exp_id = self._butler.get('ccdExposureId', dataId=data_id)
        return ccd_exp_id

    def cutout_from_pos(self, params: dict):
        """ Get cutout of source image by supported SODA shapes:
                POS: CIRCLE, RANGE, POLYGON
                LSST extension: BRECT

        Per SODA spec, all longitude and latitude (plus the radius of the
        CIRCLE) are expressed in degrees in ICRS.

        Parameters
        ----------
        params: `dict`
            the POS parameter.
        Returns
        -------
        cutout: `afwImage.Exposure`

        """
        _pos = params["POS"]
        _id = params["ID"]
        db, ds, filt = _id.split(".")
        # allow both space and comma as delimiter in values
        pos_items = _pos.replace(",", " ").split()
        shape = pos_items[0]
        if shape == "BRECT":
            if len(pos_items) < 6:
                raise Exception("BRECT: invalid number of values")
            ra, dec = float(pos_items[1]), float(pos_items[2])
            w, h = float(pos_items[3]), float(pos_items[4])
            unit_size = pos_items[5]
            cutout = self.cutout_from_nearest(ra, dec, w, h, unit_size, filt)
            return cutout
        elif shape == "CIRCLE":
            if len(pos_items) < 4:
                raise Exception("CIRCLE: invalid number of values")
            ra, dec = float(pos_items[1]), float(pos_items[2])
            radius = float(pos_items[3])
            # convert from deg to pixels by wcs (ICRS)
            q_result = self._metaservget.nearest_image_containing(ra, dec, filt)
            data_id = self._data_id_from_qr(q_result)
            metadata = self._metadata_from_data_id(data_id)
            wcs = afwGeom.makeSkyWcs(metadata, strip=False)
            r_arcsecs = radius * 3600
            pix_r = int(r_arcsecs / wcs.getPixelScale().asArcseconds())
            ss = SpanSet.fromShape(pix_r) # defaults to circle
            ss_width = ss.getBBox().getWidth()
            src_image = self.cutout_from_nearest(ra, dec, ss_width, ss_width,
                                                 "pixel", filt)
            src_cutout = src_image.getMaskedImage()
            circle_cutout = afwImage.MaskedImageF(src_cutout.getBBox())
            spanset = SpanSet.fromShape(pix_r, Stencil.CIRCLE,
                                        offset=src_cutout.getXY0() +
                                               afwGeom.Extent2I(pix_r, pix_r))
            spanset.copyMaskedImage(src_cutout, circle_cutout)
            # make an Exposure cutout with WCS info
            cutout = afwImage.ExposureF(circle_cutout,
                                            afwImage.ExposureInfo(wcs))
            return cutout
        elif shape == "RANGE":
            if len(pos_items) < 5:
                raise Exception("RANGE: invalid number of values")
            # convert the pair of (ra,dec) to bbox
            ra1, ra2 = float(pos_items[1]), float(pos_items[2])
            dec1, dec2 = float(pos_items[3]), float(pos_items[4])
            box = afwGeom.Box2D(afwGeom.Point2D(ra1, dec1),
                                  afwGeom.Point2D(ra2, dec2))
            # convert from deg to arcsec
            w = box.getWidth()*3600
            h = box.getHeight()*3600
            # compute the arithmetic center (ra, dec) of the range
            ra = (ra1 + ra2) / 2
            dec = (dec1 + dec2) / 2
            cutout = self.cutout_from_nearest(ra, dec, w, h, "arcsec", filt)
            return cutout
        elif shape == "POLYGON":
            if len(pos_items) < 7:
                raise Exception("POLYGON: invalid number of values")
            vertices = []
            pos_items.pop(0)
            for long, lat in zip(pos_items[::2], pos_items[1::2]):
                pt = afwGeom.Point2D(float(long), float(lat))
                vertices.append(pt)
            polygon = afwGeom.Polygon(vertices)
            center = polygon.calculateCenter()
            ra, dec = center.getX(), center.getY()
            # afw limitation: can only return the bbox of the polygon
            bbox = polygon.getBBox()
            # convert from 'deg' to 'arcsec'
            w = bbox.getWidth()*3600
            h = bbox.getHeight()*3600
            cutout = self.cutout_from_nearest(ra, dec, w, h, "arcsec", filt)
            return cutout

    @staticmethod
    def cutout_from_exposure(src_image, ra, dec, width, height, unit="pixel"):
        """ Get the Exposure cutout including wcs headers.

        Parameters
        ----------
        src_image : `afwImage.Exposure`
            the source image.
        ra : `float`
            in degrees.
        dec : `float`
            in degrees.
        width : int
            in pixels.
        height : int
            in pixels

        Returns
        -------
        cutout : `afwImage.Exposure`

        """
        if unit == "arcsec":
            # pixel scale is defined as Angle/pixel
            wcs = src_image.getWcs()
            ps = wcs.getPixelScale().asArcseconds()
            if ps != 0:
                width = width / ps
                height = height / ps
            else:
                raise Exception("Unexpected: pixel scale = 0")
        center = afwGeom.SpherePoint(ra, dec, afwGeom.degrees)
        size = afwGeom.Extent2I(width, height)
        cutout = src_image.getCutout(center, size)
        return cutout

    def _cutout_by_data_id(self, data_id, ra, dec, width, height, unit="pixel"):
        # check to see if q_results is empty
        if data_id is None:
            return None
        # Return an image by data ID through the butler.
        image = self._image_from_butler(data_id)
        if image is None:
            # @todo html error handling see DM-1980
            return None
        if isinstance(image, afwImage.Exposure):
            # only with exposures
            cutout = ImageGetter.cutout_from_exposure(image, ra, dec, width,
                                                      height, unit)
        else:
            cutout = self._apply_cutout(data_id, image, ra, dec, width, height,
                                        unit)
        return cutout

    def _apply_cutout(self, data_id, src_img, ra, dec, width, height,
                      unit="pixel"):
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
        src_img : `afwImage.Exposure`
            the source image.
        data_id : `dict`
            the image id.
        ra : float
        dec :  float
            in degrees.
        height, width : float, float
            Height and width are in pixels, by default.

        Returns
        -------
        cutout: `afwImage.Exposure`

        """
        self._log.debug("apply_cutout %f %f %f %f", ra, dec, width, height)
        # False: do not remove FITS keywords from metadata
        wcs = None
        if isinstance(src_img, afwImage.Exposure):
            wcs = src_img.getWcs()
        elif data_id:
            # Get the metadata for the source image.
            metadata = self._metadata_from_data_id(data_id)
            wcs = afwGeom.makeSkyWcs(metadata, strip=False)
        if wcs is None:
            raise Exception("wcs missing in source image")
        radec = afwGeom.SpherePoint(ra, dec, afwGeom.degrees)
        xy_wcs = wcs.skyToPixel(radec)
        xy_center_x = xy_wcs.getX()
        xy_center_y = xy_wcs.getY()
        self._log.debug("ra=%f dec=%f xy_center=(%f,%f)",
                        ra, dec, xy_center_x, xy_center_y)
        if unit == "arcsec":
            # pixel scale is defined as Angle/pixel
            ps = wcs.getPixelScale().asArcseconds()
            if ps != 0:
                width = width / ps
                height = height / ps
            else:
                self._log.debug("pixel scale = 0!")
        cutout = self.cutout_from_src(src_img, xy_center_x, xy_center_y, width,
                                 height, wcs)

        return cutout

    def _metadata_from_data_id(self, data_id):
        # Return the metadata for the query results in qResults.
        metadata = self._butler.get(self._imagedataset_md(), dataId=data_id)
        return metadata

    def _image_from_butler(self, data_id, bbox=None):
        # Retrieve the image through the Butler using data id.
        self._log.debug("_image_from_butler data_id:{}".format(data_id))
        if bbox:
            image = self._butler.get(self._imagedataset_sub(), bbox=bbox,
                                     dataId=data_id, immediate=True)
        else:
            image = self._butler.get(self._imagedataset_type, dataId=data_id)
        return image

    def _imagedataset_md(self):
        # Return the butler policy name to retrieve metadata
        return self._imagedataset_type + "_md"

    def _imagedataset_sub(self):
        # Return the dataset type for sub-images
        return self._imagedataset_type + "_sub"


    def _cutout_from_src(self, src_image, xy_center_x, xy_center_y, width,
                         height, wcs):
        # Returns an image cutout from the source image.
        # srcImage - Source image.
        # xy_center - The center of region to cutout in pixels.
        # width - The width in pixels.
        # height - The height in pixels.
        # height and width trimmed if they go past the edge of source image.
        # First, center the cutout image.
        pix_ulx = int(xy_center_x - width / 2.0)
        pix_uly = int(xy_center_y - height / 2.0)
        xy_center = afwGeom.Point2I(pix_ulx, pix_uly)
        log.debug("xy_center={}".format(xy_center))
        src_box = src_image.getBBox()
        # assuming both src_box and xy_center to be in Box2I
        co_box = afwGeom.Box2I(xy_center,
                               afwGeom.Extent2I(int(width), int(height)))
        if co_box.overlaps(src_box):
            co_box.clip(src_box)
        else:
            self._log.debug(
                "cutout image wanted is OUTSIDE source image -> None")
            raise Exception("non-overlapping cutout bbox")
        if isinstance(src_image, afwImage.ExposureF):
            self._log.debug(
                "co_box pix_ulx={} pix_end_x={} pix_uly={} pix_end_y={}"
                .format(pix_ulx, pix_ulx + width, pix_uly, pix_uly + height))
            # image will keep wcs from source image
            cutout = afwImage.ExposureF(src_image, co_box)
        elif isinstance(src_image, afwImage.ExposureU):
            cutout = afwImage.ExposureU(src_image, co_box)
        else:
            raise Exception("Unexpected source image object type")
        return cutout

    def _data_id_from_qr(self, q_results):
        # identify and fetch the data ID from 1 of 2 defined sets
        ln = q_results[0]
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
