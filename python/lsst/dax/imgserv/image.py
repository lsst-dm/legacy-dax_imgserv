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


class Image:
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
        """Get the full image of the data id.

        Parameters
        ----------
        img_getter : `ImageGetter`
        params: `dict`
            contain the dataId.
        Returns
        -------
        image: `afwImage`
            the full image (FITS).
        """
        image = img_getter.full_image_from_data_id(params)
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
    def cutout_from_id(cls, img_getter: ImageGetter, params: dict) \
            -> afwImage:
        """ Get cutout through Gen3 Butler.

        Parameters
        ----------
        img_getter : getimage.imagegetter.ImageGetter
        params: dict
                contains the image id.

        Returns
        -------
        lsst.afw.Image

        """
        cutout = img_getter.cutout_from_id(params)
        return cutout

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
        data_id = img_getter.data_id_from_params(params)
        ccd_exp_id = img_getter.ccd_exp_id_from_data_id(data_id)
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
