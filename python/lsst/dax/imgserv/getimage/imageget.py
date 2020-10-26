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

"""
import math
from urllib.parse import urlparse, parse_qs
import etc.imgserv.imgserv_config as imgserv_config

import lsst.geom as Geom
import lsst.afw.image as afwImage
from lsst.afw.geom import SpanSet, Stencil, makeSkyWcs, Polygon

import lsst.log as log

from .skymapImage import SkymapImage
from ..exceptions import ImageNotFoundError, UsageError


class ImageGetter:
    """Provide operations to retrieve images including cutouts from the
    specified image repository through the passed-in butler and metaget.

    Parameters
    ----------
        config: `dict`
            the application configuration.
        butlerget : `locateImage.ButlerGet`
            the butler instance and config info.
        metaget : `locateImage.MetaGet`
            provides access to image meta data.
    """

    def __init__(self, config, butlerget, metaget):
        self._config = config
        self._butler = butlerget.butler
        self._butler_keys = sorted(butlerget.butler_keys)
        self._ds_type = butlerget.butler_ds
        self._metaget = metaget

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
        # 0.01: default value in arcsec
        ds_type = self._ds_type
        q_result = self._metaget.nearest_image_contains(ds_type, ra, dec, 0.01, filt)
        if not q_result:
            raise ImageNotFoundError("Image Query Returning None")
        else:
            data_id = self.data_id_from_obscore(q_result)
            image = self._image_from_butler(data_id)
            return image

    def full_image_from_data_id(self, params: dict):
        """ Returns image from specified data id

        Parameters
        ----------
        params : `dict`
            parameters that contain the data id.

        Returns
        -------
        image : `afwImage.Exposure`

        """
        data_id = self.data_id_from_params(params)
        image = self._butler.get(self._ds_type, dataId=data_id)
        return image

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
        radius = math.sqrt((width/2)**2+(height/2)**2)
        if unit != "deg":
            if unit == "arcsec" or unit == "arsecond":
                radius = radius / 3600
            else:
                raise UsageError("Invalid unit type for size")
        q_result = self._metaget.nearest_image_contains(self._ds_type, ra, dec, radius, filt)
        if not q_result:
            raise ImageNotFoundError("Empty result returned for image query")
        data_id = self.data_id_from_obscore(q_result)
        if not data_id:
            raise ImageNotFoundError("No dataId found in query result")
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
        skymap = SkymapImage(self._butler, skymap_id)
        center_coord = Geom.SpherePoint(ra, dec, Geom.degrees)
        cutout = skymap.get(center_coord, width, height, filt, unit)
        return cutout

    def cutout_from_id(self, params: dict) -> afwImage:
        """ Get cutout through Butler Gen3.

        Parameters
        ----------
        params: `dict`
            data_id (one of):
                1) instrument, detector, visit
                2) band, skymap, tract, patch
                3) instrument, detector, exposure
            POS: `str`
                the cutout specification.
        Returns
        -------
        lsst.afw.Image

        """
        ds_type = params.get("dsType")
        pos = params.get("POS", None)
        if pos is None:
            raise UsageError("Missing POS parameter")
        data_id = self.data_id_from_params(params)
        image = self._butler.get(ds_type, data_id)
        return self.cutout_from_pos(params, image, data_id)

    def cutout_from_pos(self, params: dict, src_img=None, data_id=None):
        """ Get cutout of source image by supported SODA shapes:
                POS: CIRCLE, RANGE, POLYGON
                LSST extension: BRECT

        Per SODA spec, all longitude and latitude (plus the radius of the
        CIRCLE) are expressed in degrees in ICRS.

        Parameters
        ----------
        src_img: `afwImage.Exposure`
            the source image if any.
        params: `dict`
            the POS parameter.
        data_id: `dict`
            the data id.
        Returns
        -------
        cutout: `afwImage.Exposure`

        """
        pos = params["POS"]
        ds_type = self._ds_type
        filt = params.get("filter", None)
        # allow both space and comma as delimiter in values
        pos_items = pos.replace(",", " ").split()
        shape = (pos_items[0]).upper()
        if shape == "BRECT":
            nargs = len(pos_items)
            if nargs < 5:
                raise UsageError("BBOX: invalid input parameters")
            ra, dec = float(pos_items[1]), float(pos_items[2])
            w, h = float(pos_items[3]), float(pos_items[4])
            if nargs == 6:
                unit_size = pos_items[5]
            else:
                unit_size = "deg"  # default
            if src_img is None:
                cutout = self.cutout_from_nearest(ra, dec, w, h, unit_size, filt)
            else:
                cutout = self._cutout_from_image(src_img, ra, dec, w, h, unit_size, data_id)
            return cutout
        elif shape == "CIRCLE":
            if len(pos_items) < 4:
                raise UsageError("CIRCLE: invalid number of values")
            ra, dec = float(pos_items[1]), float(pos_items[2])
            radius = float(pos_items[3])
            if src_img is None:
                q_result = self._metaget.nearest_image_contains(ds_type, ra, dec, radius, filt)
                data_id = self.data_id_from_obscore(q_result)
            wcs = self._get_wcs_from_butler(data_id)
            # convert from deg to pixels by wcs (ICRS)
            pix_r = int(radius / wcs.getPixelScale().asDegrees())
            ss = SpanSet.fromShape(pix_r, Stencil.CIRCLE)
            ss_width = ss.getBBox().getWidth()
            ss_height = ss.getBBox().getHeight()
            # create a sub image of bbox with all metadata from source image
            cutout = self._cutout_by_data_id(data_id, ra, dec, ss_width, ss_height, "pixel", wcs)
            ss_circle = SpanSet.fromShape(pix_r, Stencil.CIRCLE, offset=cutout.getXY0() + Geom.Extent2I(
                pix_r, pix_r))
            no_data = cutout.getMask().getMaskPlane("NO_DATA")
            ss_bbox = SpanSet(ss_circle.getBBox())
            ss_nodata = ss_bbox.intersectNot(ss_circle)
            # set region outside circle to NO_DATA (or 8) bit value
            ss_nodata.setImage(cutout.getImage(), no_data)
            return cutout
        elif shape == "RANGE":
            if len(pos_items) < 5:
                raise UsageError("RANGE: invalid number of values")
            # convert the pair of (ra,dec) to bbox
            ra1, ra2 = float(pos_items[1]), float(pos_items[2])
            dec1, dec2 = float(pos_items[3]), float(pos_items[4])
            box = Geom.Box2D(Geom.Point2D(ra1, dec1), Geom.Point2D(ra2, dec2))
            # convert from deg to arcsec
            w = box.getWidth() * 3600
            h = box.getHeight() * 3600
            # compute the arithmetic center (ra, dec) of the range
            ra = (ra1 + ra2) / 2
            dec = (dec1 + dec2) / 2
            if src_img is None:
                cutout = self.cutout_from_nearest(ra, dec, w, h, "arcsec", filt)
            else:
                cutout = self._cutout_from_image(src_img, ra, dec, w, h, "arcsec", data_id)
            return cutout
        elif shape == "POLYGON":
            if len(pos_items) < 7:
                raise UsageError("POLYGON: invalid number of values")
            vertices = []
            pos_items.pop(0)
            for long, lat in zip(pos_items[::2], pos_items[1::2]):
                pt = Geom.Point2D(float(long), float(lat))
                vertices.append(pt)
            polygon = Polygon(vertices)
            center = polygon.calculateCenter()
            ra, dec = center.getX(), center.getY()
            # afw limitation: can only returns the bbox of the polygon
            bbox = polygon.getBBox()
            w = bbox.getWidth()
            h = bbox.getHeight()
            if src_img is None:
                cutout = self.cutout_from_nearest(ra, dec, w, h, "deg", filt)
            else:
                cutout = self._cutout_from_image(src_img, ra, dec, w, h, "deg", data_id)
            return cutout
        else:
            raise UsageError("Invalid shape in POS")

    def _cutout_from_image(self, src_image, ra, dec, width, height, unit="pixel", data_id=None):
        """ Get the Exposure cutout including wcs headers.

        Parameters
        ----------
        src_image : `afwImage.Exposure`
            the source image.
        ra : `float`
            in degrees.
        dec : `float`
            in degrees.
        width : `float`
            the width.
        height : `float`
            the height.

        Returns
        -------
        cutout : `afwImage.Exposure`

        """
        if unit == "arcsec":
            # pixel scale is defined as Angle/pixel
            wcs = src_image.getWcs()
            ps = wcs.getPixelScale().asArcseconds()
            width = width / ps
            height = height / ps
        center = Geom.SpherePoint(ra, dec, Geom.degrees)
        size = Geom.Extent2I(width, height)
        if isinstance(src_image, afwImage.Exposure):
            cutout = src_image.getCutout(center, size)
        else:
            cutout = self._apply_cutout(data_id, src_image, ra, dec, width, height, unit)
        return cutout

    def _cutout_by_data_id(self, data_id, ra, dec, width, height, unit="pixel", wcs=None):
        if not wcs:
            wcs = self._get_wcs_from_butler(data_id)
        ps = wcs.getPixelScale().asDegrees()
        # check to see if size exceeds maximum allowed
        if unit in ["pixel", "pix", "px"]:
            cutout_area = width * height * ps**2
        elif unit in ["arcsec", "arsecond"]:
            cutout_area = width * height / 3600**2
            width = width / 3600 / ps
            height = height / 3600 / ps
        elif unit in ["deg", "degree", "degrees"]:
            cutout_area = width * height
            width = width / ps
            height = height / ps
        else:
            raise UsageError("Invalid unit for cutout size")
        if cutout_area > imgserv_config.MAX_IMAGE_CUTOUT_SIZE:
            msg = f"Requested image exceeded " \
                  f"{imgserv_config.MAX_IMAGE_CUTOUT_SIZE} squared degrees"
            raise UsageError(msg)
        pos = Geom.SpherePoint(ra, dec, Geom.degrees)
        xy = Geom.PointI(wcs.skyToPixel(pos))
        cutout_size = Geom.Extent2I(width, height)
        cutout_box = Geom.Box2I(xy - cutout_size//2, cutout_size)
        # cutout_box must be in pixels
        cutout = self._image_from_butler(data_id, cutout_box)
        return cutout

    def _apply_cutout(self, data_id, src_img, ra, dec, width, height, unit="pixel"):
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
        log.debug("apply_cutout %f %f %f %f", ra, dec, width, height)
        # False: do not remove FITS keywords from metadata
        wcs = None
        if isinstance(src_img, afwImage.Exposure):
            wcs = src_img.getWcs()
        elif data_id:
            wcs = self._get_wcs_from_butler(data_id)
        if wcs is None:
            raise Exception("wcs missing in source image")
        radec = Geom.SpherePoint(ra, dec, Geom.degrees)
        xy_wcs = wcs.skyToPixel(radec)
        xy_center_x = xy_wcs.getX()
        xy_center_y = xy_wcs.getY()
        log.debug("ra=%f dec=%f xy_center=(%f,%f)", ra, dec, xy_center_x, xy_center_y)
        if unit == "arcsec":
            # pixel scale is defined as Angle/pixel
            ps = wcs.getPixelScale().asArcseconds()
            width = width / ps
            height = height / ps
        pix_ulx = int(xy_center_x - width / 2.0)
        pix_uly = int(xy_center_y - height / 2.0)
        xy_center = Geom.Point2I(pix_ulx, pix_uly)
        log.debug("xy_center={}".format(xy_center))
        src_box = src_img.getBBox()
        # assuming both src_box and xy_center to be in Box2I
        co_box = Geom.Box2I(xy_center, Geom.Extent2I(int(width), int(height)))
        if co_box.overlaps(src_box):
            co_box.clip(src_box)
        else:
            log.debug("cutout image wanted is OUTSIDE source image -> None")
            raise UsageError("non-overlapping cutout bbox")
        if isinstance(src_img, afwImage.ExposureF):
            log.debug("co_box pix_ulx={} pix_end_x={} pix_uly={} pix_end_y={}".format(pix_ulx,
                                                                                      pix_ulx + width,
                                                                                      pix_uly,
                                                                                      pix_uly + height))
            # image will keep wcs from source image
            cutout = afwImage.ExposureF(src_img, co_box)
        elif isinstance(src_img, afwImage.ExposureU):
            cutout = afwImage.ExposureU(src_img, co_box)
        else:
            raise Exception("Unexpected source image object type")
        return cutout

    def _get_wcs_from_butler(self, data_id):
        wcs = self._butler.get(self._ds_type + ".wcs", dataId=data_id)
        return wcs

    def _image_from_butler(self, data_id, bbox=None):
        # Retrieve the image through the Butler using data id.
        log.debug("_image_from_butler data_id:{}".format(data_id))
        try:
            image = self._butler.get(self._ds_type, dataId=data_id, parameters={"bbox": bbox}, immediate=True)
        except Exception as e:
            raise ImageNotFoundError("butler failed to get image") from e
        return image

    @staticmethod
    def data_id_from_params(params):
        if set(params.keys()) >= {"instrument", "detector", "visit"}:
            data_id = {"visit": params.get("visit"),
                       "detector": params.get("detector"),
                       "instrument": params.get("instrument")}
        elif set(params.keys()) >= {"band", "skymap", "tract", "patch"}:
            data_id = {"band": params.get("band"),
                       "skymap": params.get("skymap"),
                       "tract": params.get("tract"),
                       "patch": params.get("patch")}
        elif set(params.keys()) >= {"instrument", "detector", "exposure"}:
            data_id = {"instrument": params.get("instrument"),
                       "detector": params.get("detector"),
                       "exposure": params.get("exposure")}
        else:
            raise UsageError("Invalid dataId")
        return data_id

    @staticmethod
    def data_id_from_obscore(q_results):
        if len(q_results) != 1:
            # TODO: enhance handling for multiple results
            raise UsageError("Info: overlapping images in query result")
        ln = q_results[0]
        vo_item = ImageGetter._find_item("ivo://", ln)
        data_id = ImageGetter._extract_data_id(vo_item)
        return data_id

    @staticmethod
    def _find_item(term, fields):
        for x in fields:
            if type(x) == str:
                if term in x:
                    return x
            elif type(x) == int or type(x) == float:
                if term == x:
                    return x

    @staticmethod
    def _extract_data_id(obs_pub_did):
        o = urlparse(obs_pub_did)
        did = parse_qs(o.query)
        data_id = {}
        for k in did:
            v = did[k][0]
            if v.isnumeric():
                data_id[k] = int(v)
            else:
                data_id[k] = v
        return data_id
