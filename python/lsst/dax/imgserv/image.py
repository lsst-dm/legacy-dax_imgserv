#
# LSST Data Management System
# Copyright 2017-2019 LSST/AURA.
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
This module implements the Image class that uses passed-in imagegetter
to retrieve the image/cutout as specified.

"""


class Image(object):
    """ Image module maps a request and its parameters per JSON schema to the
        corresponding imagegetter method.
    """
    @classmethod
    def full_nearest(cls, image_getter, params):
        """Get image nearest center.

        Parameters
        ----------
        image_getter : getimage.imagegetter.ImageGetter
        params: dict
            center.x, center.y, center.unit, filter

        Returns
        -------
        lsst.afw.image
            the full image (FITS).
        """
        x = float(params.get("center.x"))
        y = float(params.get("center.y"))
        unit = params.get("center.unit")
        filt = params.get("nearest.filter") or params.get("filter")
        image = image_getter.full_nearest(x, y, unit, filt)
        return image

    @classmethod
    def full_from_data_id(cls, image_getter, params):
        """Get image from data id.

        Parameters
        ----------
        image_getter : getimage.imagegetter.ImageGetter
        data id: 1) run, camcol, field, filter
`                2) tract, patch_x, patch_y, filter
        Returns
        -------
        lsst.afw.image
            the full image (FITS).
        """
        if all (k in params.keys() for k in ["run", "camcol", "field",
            "filter"]):
            run = int(params.get("run"))
            camcol = int(params.get("camcol"))
            field = int(params.get("field"))
            filt = params.get("filter")
            image = image_getter.full_from_data_id_by_run(run, camcol, field,
                    filt)
        elif all (k in params.keys() for k in ["tract", "patch_x", "patch_y",
            "filter"]):
            tract = int(params.get("tract"))
            patch_x = int(params.get("patch_x"))
            patch_y = int(params.get("patch_y"))
            filt = params.get("filter")
            image = image_getter.full_from_data_id_by_tract(tract, patch_x,
                    patch_y, filt)
        elif all (k in params.keys() for k in ["tract", "patch", "filter"]):
            tract = int(params.get("tract"))
            patch_x, patch_y = params.get("patch").split(",")
            filt = params.get("filter")
            image = image_getter.full_from_data_id_by_tract(tract, patch_x,
                    patch_y, filt)
        else:
            raise Exception("invalid data id")
        return image


    @classmethod
    def full_from_science_id(cls, image_getter, params):
        """Get image from the science id.

        Parameters
        ----------
        image_getter : getimage.imagegetter.ImageGetter
        science_id

        Returns
        -------
        lsst.afw.image
            the full image (FITS).
        """
        science_id = int(params.get("science_id"))
        image = image_getter.full_from_science_id(science_id)
        return image

    @classmethod
    def cutout_from_nearest(cls, image_getter, params):
        """Get cutout nearest the center of size.

        Parameters
        ----------
        image_getter : getimage.imagegetter.ImageGetter
        params: dict
            center.x, center.y, center-unit, size.x, size.y, size.unit, filter

        Returns
        -------
        lsst.afw.image
            the cutout image (FITS).
        """
        center_x = float(params.get("center.x"))
        center_y = float(params.get("center.y"))
        center_unit = params.get("center.unit")
        size_x = float(params.get("size.x"))
        size_y = float(params.get("size.y"))
        size_unit = params.get("size.unit")
        filt = params.get("nearest.filter") or params.get("filter")
        image = image_getter.cutout_from_nearest(center_x, center_y,
                                                 center_unit, size_x, size_y,
                                                 size_unit, filt)
        return image

    @classmethod
    def cutout_from_data_id(cls, image_getter, params):
        """Get cutout image from data_id[run, camcol, field, filter]
        at specified center and size.

        Parameters
        ----------
        image_getter : getimage.imagegetter.ImageGetter
        data id: 1) run, camcol, field, filter,
                 2) tract, patch, filter
                 3) tract, patch_x, patch_y, filter
        center.y, center.y, center.unit, size.x,
        size.y, size.unit

        Returns
        -------
        lsst.afw.image
            the cutout image (FITS).
        """
        if all (k in params.keys() for k in ("run", "camcol", "field", "filter")):
            run = int(params.get("run"))
            camcol = int(params.get("camcol"))
            field = int(params.get("field"))
            filt = params.get("filter")
            center_x = float(params.get("center.x"))
            center_y = float(params.get("center.y"))
            center_unit = params.get("center.unit")
            size_x = float(params.get("size.x"))
            size_y = float(params.get("size.y"))
            size_unit = params.get("size.unit")
            image = image_getter.cutout_from_data_id_by_run(run, camcol, field, filt,
                center_y, center_y, center_unit,  size_x, size_y, size_unit)
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
            center_unit = params.get("center.unit")
            size_x = float(params.get("size.x"))
            size_y = float(params.get("size.y"))
            size_unit = params.get("size.unit")
            image = image_getter.cutout_from_data_id_by_tract(tract, patch_x, patch_y, filt,
                    center_x, center_y, center_unit, size_x, size_y, size_unit)
        else:
            raise Exception("invalid data id")
        return image

    @classmethod
    def cutout_from_science_id(cls, image_getter, params):
        """Get cutout image from science id of specified center and size.

        Parameters
        ----------
        image_getter : getimage.imagegetter.ImageGetter
        science_id, center.y, center.y, center.unit, size.x, size.y, size.unit

        Returns
        -------
        lsst.afw.Image or None
        """
        science_id = int(params.get("science_id"))
        center_x = float(params.get("center.x"))
        center_y = float(params.get("center.y"))
        center_unit = params.get("center.unit")
        size_x = float(params.get("size.x"))
        size_y = float(params.get("size.y"))
        size_unit = params.get("size.unit")
        image = image_getter.cutout_from_science_id(science_id, center_x,
                center_y, center_unit, size_x, size_y, size_unit)
        return image

    @classmethod
    def cutout_from_skymap_id(cls, image_getter, params):
        """Get cutout image from skymap id of specified center and size.

        Parameters
        ----------
        image_getter : getimage.imagegetter.ImageGetter
        skymap_id, center.y, center.y, center.unit, size.x, size.y, size.unit

        Returns
        -------
        lsst.afw.Image or None
        """
        skymap_id = params.get("skymap_id")
        filt = params.get("filter")
        center_x = float(params.get("center.x"))
        center_y = float(params.get("center.y"))
        center_unit = params.get("center.unit")
        size_x = float(params.get("size.x"))
        size_y = float(params.get("size.y"))
        size_unit = params.get("size.unit")
        image = image_getter.cutout_from_skymap_id(skymap_id, filt, center_x,
                center_y, center_unit, size_x, size_y, size_unit)
        return image

    @classmethod
    def dataid_from_scienceid(cls, image_getter, params):
        """Get the data id for the correspnding science id.

        Parameters
        ----------
        image_getter: getimage.imagegetter.ImageGetter
        params: dict

        Returns
        -------
        dict - the data id.

        """
        science_id = int(params.get("ccdexpid"))
        data_id = image_getter.data_id_from_science_id(science_id)
        return data_id

    @classmethod
    def scienceid_from_dataid(cls, image_getter, params):
        """The science id for the corresponding data id.

        Parameters
        ----------
        image_getter: getimage.imagegetter.ImageGetter
        params: dict

        Returns
        -------
        int - the science id.

        """
        data_id = cls._get_data_id(params)
        science_id = image_getter.scienceid_from_dataid(data_id)
        return science_id

    @classmethod
    def cutout_from_pos(cls, image_getter, params):
        """ This implements the shapes per SODA """
    #ToDo

    @classmethod
    def _get_data_id(cls, params):
        """Returns the data id from the params."""
        data_id={}
        if "run" in params.keys():
            data_id["run"] = int(params.get("run"))
            data_id["camcol"] = int(params.get("camcol"))
            data_id["field"] = int(params.get("field"))
            data_id["filter"] = params.get("filter")
        elif "tract" in params.keys():
            data_id["tract"] = int(params.get("tract"))
            data_id["filter"] = params.get("filter")
            if params.get("patch"):
                x,y = params.get("patch").split(",")
                data_id["patch_x"] = int(x)
                data_id["patch_y"] = int(y)
            else:
                patch_x = int(params.get("patch_x"))
                patch_y = int(params.get("patch_y"))
        else:
            raise Exception("invalid data id input")
        return data_id


