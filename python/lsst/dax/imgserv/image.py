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
This module implements the Image class that uses passed-in imagegetter
to retrieve the image/cutout as specified.

"""
import lsst.afw.image as afwImage
from .getimage.imageget import ImageGetter


class Image(object):
    """ Image module maps a request and its parameters per JSON schema to the
        corresponding ImageGetter method.

    """
    @classmethod
    def full_nearest(cls, img_getter: ImageGetter, params: dict) -> afwImage:
        """Get image nearest center.

        Parameters
        ----------
        img_getter : `ImageGetter`
        params: dict
            center.x, center.y, center.unit, filter

        Returns
        -------
        image: `afwImage`
            the full image (FITS).
        """
        x = float(params.get("center.x"))
        y = float(params.get("center.y"))
        filt = params.get("nearest.filter") or params.get("filter")
        image = img_getter.full_nearest(x, y, filt)
        return image

    @classmethod
    def full_from_data_id(cls, img_getter: ImageGetter, params: dict) -> \
            afwImage:
        """Get image from data id.

        Parameters
        ----------
        img_getter : `ImageGetter`
        params: `dict`
            data id: 1) run, camcol, field, filter
`                   2) tract, patch_x, patch_y, filter
        Returns
        -------
        image: `afwImage`
            the full image (FITS).
        """
        if all (k in params.keys() for k in ["run", "camcol", "field",
            "filter"]):
            run = int(params.get("run"))
            camcol = int(params.get("camcol"))
            field = int(params.get("field"))
            filt = params.get("filter")
            image = img_getter.full_from_data_id_by_run(run, camcol, field,
                    filt)
        elif all (k in params.keys() for k in ["tract", "patch_x", "patch_y",
            "filter"]):
            tract = int(params.get("tract"))
            patch_x = int(params.get("patch_x"))
            patch_y = int(params.get("patch_y"))
            filt = params.get("filter")
            image = img_getter.full_from_data_id_by_tract(tract, patch_x,
                    patch_y, filt)
        elif all (k in params.keys() for k in ["tract", "patch", "filter"]):
            tract = int(params.get("tract"))
            patch_x, patch_y = params.get("patch").split(",")
            filt = params.get("filter")
            image = img_getter.full_from_data_id_by_tract(tract, patch_x,
                    patch_y, filt)
        else:
            raise Exception("invalid data id")
        return image


    @classmethod
    def full_from_ccd_exp_id(cls, img_getter: ImageGetter, params: dict) -> \
            afwImage:
        """Get image from the science id.

        Parameters
        ----------
        img_getter : `ImageGetter`
        params: `dict`

        Returns
        -------
        image: afwImage
            the full image (FITS).
        """
        ccd_exp_id = int(params.get("ccd_exp_id"))
        image = img_getter.full_from_ccd_exp_id(ccd_exp_id)
        return image

    @classmethod
    def cutout_from_nearest(cls, img_getter: ImageGetter, params: dict) -> \
            afwImage:
        """Get cutout nearest the center of size.

        Parameters
        ----------
        img_getter : getimage.imagegetter.ImageGetter
        params: dict
            center.x, center.y, center-unit, size.x, size.y, size.unit, filter

        Returns
        -------
        lsst.afw.image
            the cutout image (FITS).
        """
        center_x = float(params.get("center.x"))
        center_y = float(params.get("center.y"))
        size_x = float(params.get("size.x"))
        size_y = float(params.get("size.y"))
        size_unit = params.get("size.unit")
        filt = params.get("nearest.filter") or params.get("filter")
        image = img_getter.cutout_from_nearest(center_x, center_y,
                                                 size_x, size_y,
                                                 size_unit, filt)
        return image

    @classmethod
    def cutout_from_data_id(cls, img_getter: ImageGetter, params: dict) -> \
            afwImage.Exposure:
        """Get cutout image from dataid[run, camcol, field, filter]
        at specified center and size.

        Parameters
        ----------
        img_getter : getimage.imagegetter.ImageGetter
        params : `dict`
            data id: 1) run, camcol, field, filter,
                 2) tract, patch, filter
                 3) tract, patch_x, patch_y, filter
        center.y, center.y, center.unit, size.x,
        size.y, size.unit

        Returns
        -------
        image: `afwImage`
            the cutout image (FITS).
        """
        if all (k in params.keys() for k in ("run", "camcol", "field", "filter")):
            run = int(params.get("run"))
            camcol = int(params.get("camcol"))
            field = int(params.get("field"))
            filt = params.get("filter")
            center_x = float(params.get("center.x"))
            center_y = float(params.get("center.y"))
            size_x = float(params.get("size.x"))
            size_y = float(params.get("size.y"))
            size_unit = params.get("size.unit")
            image = img_getter.cutout_from_data_id_by_run(run, camcol, field,
                                                          filt,
                center_x, center_y, size_x, size_y, size_unit)
        elif "tract" in params.keys():
            tract = int(params.get("tract"))
            if params.get("patch"):
                x, y = params.get("patch").split(",")
                patch_x, patch_y = int(x), int(y)
            else:
                patch_x = int(params.get("patch_x"))
                patch_y = int(params.get("patch_y"))
            filt = params.get("nearest.filter") or params.get("filter")
            center_x = float(params.get("center.x"))
            center_y = float(params.get("center.y"))
            size_x = float(params.get("size.x"))
            size_y = float(params.get("size.y"))
            size_unit = params.get("size.unit")
            image = img_getter.cutout_from_data_id_by_tract(tract, patch_x,
                                                            patch_y, filt,
                    center_x, center_y, size_x, size_y, size_unit)
        else:
            raise Exception("invalid data id")
        return image

    @classmethod
    def cutout_from_ccd_exp_id(cls, img_getter: ImageGetter, params: dict) \
            -> afwImage:
        """Get cutout image from science id of specified center and size.

        Parameters
        ----------
        img_getter : getimage.imagegetter.ImageGetter
        ccd_exp_id, center.y, center.y, center.unit, size.x, size.y, size.unit

        Returns
        -------
        lsst.afw.Image or None
        """
        ccd_exp_id = int(params.get("ccd_exp_id"))
        center_x = float(params.get("center.x"))
        center_y = float(params.get("center.y"))
        size_x = float(params.get("size.x"))
        size_y = float(params.get("size.y"))
        size_unit = params.get("size.unit")
        image = img_getter.cutout_from_ccd_exp_id(ccd_exp_id, center_x,
                center_y, size_x, size_y, size_unit)
        return image

    @classmethod
    def cutout_from_skymap_id(cls, img_getter: ImageGetter, params: dict) \
            -> afwImage:
        """Get cutout image from skymap id of specified center and size.

        Parameters
        ----------
        img_getter : getimage.imagegetter.ImageGetter
        params:  `dict`
            skymap_id, center.y, center.y, size.x, size.y, size.unit

        Returns
        -------
        afwImage or None
        """
        skymapid = params.get("skymap_id")
        filt = params.get("filter")
        center_x = float(params.get("center.x"))
        center_y = float(params.get("center.y"))
        size_x = float(params.get("size.x"))
        size_y = float(params.get("size.y"))
        size_unit = params.get("size.unit")
        image = img_getter.cutout_from_skymap_id(skymapid, filt, center_x,
                center_y, size_x, size_y, size_unit)
        return image

    @classmethod
    def datai_id_from_science_id(cls, img_getter: ImageGetter, params: dict) \
            -> dict:
        """Get the data id for the corresponding science id.

        Parameters
        ----------
        img_getter: ImageGetter
        params: dict

        Returns
        -------
        dataid: `dict`

        """
        ccd_exp_id = int(params.get("ccdexpid"))
        dataid = img_getter.data_id_from_ccd_exp_id(ccd_exp_id)
        return dataid

    @classmethod
    def science_id_from_data_id(cls, img_getter: ImageGetter, params: dict) \
            -> int:
        """The science id for the corresponding data id.

        Parameters
        ----------
        img_getter: ImageGetter
        params: dict

        Returns
        -------
        science id: `int`

        """
        dataid = cls._get_dataid(params)
        ccd_exp_id = img_getter.ccd_exp_id_from_data_id(dataid)
        return ccd_exp_id

    @classmethod
    def cutout_from_pos(cls, img_getter: ImageGetter, params: dict) -> afwImage:
        """ This implements the shapes per SODA

        Parameters
        ----------
        img_getter: `ImageGetter`
        params: `dict`

        Returns
        -------
        image: `afwImage`

        """
        image = img_getter.cutout_from_pos(params)
        return image

    @classmethod
    def _get_dataid(cls, params: dict) -> dict:
        """Returns the data id from the params.

        Note: use get() so default value returned instead of an error.

        Parameters
        ----------
        params: `dict`

        Returns
        -------
        dataid: `dict`
        """
        dataid={}
        if "run" in params.keys():
            dataid["run"] = int(params.get("run"))
            dataid["camcol"] = int(params.get("camcol"))
            dataid["field"] = int(params.get("field"))
            dataid["filter"] = params.get("filter")
        elif "tract" in params.keys():
            dataid["tract"] = int(params.get("tract"))
            dataid["filter"] = params.get("filter")
            if params.get("patch"):
                x,y = params.get("patch").split(",")
                dataid["patch_x"] = int(x)
                dataid["patch_y"] = int(y)
            else:
                dataid["patch_x"] = int(params.get("patch_x"))
                dataid["patch_y"] = int(params.get("patch_y"))
        else:
            raise Exception("invalid data id input")
        return dataid


