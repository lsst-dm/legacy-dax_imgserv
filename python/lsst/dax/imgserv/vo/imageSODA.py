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


import lsst.afw.image as afw_image

from ..locateImage import get_image
from .soda.soda import SODA
from ..taskqueue.tasks import get_image_task
from ..hashutil import Hasher

""" This module implements the IVOA's SODA v1.0 for DAX ImageServ.

    All ra,dec values are expressed in ICRS degrees, by default.

    Shape:
        CIRCLE <ra> <dec> <radius>
        RANGE <ra1> <ra2> <dec1> <dec2>
        POLYGON <ra1> <dec1> ... (at least 3 pairs)
        BRECT <ra> <dec> <w> <h> <filter> <unit>
"""


class ImageSODA(SODA):
    """ Class to handle SODA operations.

    For reference, LSST camera filters (Filter, Blue Side, Red Side):
        G : 400 - 552
        R : 552 - 691
        I : 691 - 818
        Z : 818 - 922
        Y : 948 - 1060
    """
    def __init__(self, config):
        self._config = config

    def do_sync(self, params: dict) -> object:
        """ Do sync operation.

        Parameters
        ---------
        params : `dict`
            the HTTP parameters
        """
        if "POS" in params:
            resp = super().handle_pos(params)
            if isinstance(resp, tuple):
                # image is first element of the tuple
                return resp[0]
            else:
                return resp
        else:
            raise NotImplementedError("ImageSODA.do_sync(): Unsupported "
                                      "Request")

    def do_async(self, params: dict) -> object:
        """ Do async operation. Create a task for the request and enqueue for
        processing, then return the URL for user to retrieve the result.

        Parameters
        ----------
        params : `dict`
            the HTTP parameters.

        Returns
        -------
        resp: `str`
            the status.

        """
        req_key = Hasher.md5(params)
        task = get_image_task.delay(params)
        resp = {"req_key": req_key, "task_id": task.id}
        return resp

    def do_sia(self, params: dict) -> object:
        """ Do async operation.

        Parameters
        ----------
        params : `dict`
            the HTTP parameters.

        Returns
        -------
        xml: `str`

        """
        raise NotImplementedError("ImageSODA.do_sia()")

    def get_examples(self, params:dict) -> str:
        """ Get examples for this service.

        Parameters
        ----------
        params : `dict`

        Returns
        -------
        xml: `str`

        """
        return super().get_examples(params)

    def get_tables(self, params:dict) -> str:
        """ Get examples for this service.

        Parameters
        ----------
        params : `dict`

        Returns
        -------
        xml: `str`

        """
        raise NotImplementedError("ImageSODA.get_tables()")

    def get_availability(self, params: dict) -> str:
        """ Get the service availability status.

        Parameters
        ---------
        params : `dict`

        Returns
        -------
        xml: `str`

        """
        params["status"] = "true"  # xsd:boolean type
        params["service_name"] = "Image SODA"
        return super().get_availability(params)

    def get_capabilities(self, params: dict) -> str:
        """ Get the service capabilities.

        Parameters
        ----------
        params : `dict`
            the HTTP parameters.

        Returns
        -------
        xml: `str`

        """
        return super().get_capabilities(params)

    def handle_default(self, params: dict) -> afw_image:
        """Dispatch to class specific methods """
        shape = params["POS"].split()
        if shape[0] == "BRECT":
            return self.get_brect(params)
        else:
            raise TypeError("ImageSODA", "Unsupported shape", shape[0])

    def get_circle(self, params: dict) -> afw_image:
        """ Method to retrieve image cutout specified in CIRCLE.

        All input values in ICRS degrees, including the radius.

        Parameters
        ----------
        params: `dict`
            ID: <db>.<ds>.<filter>
                    db: database identifier
                    ds: dataset identifier
                    filter: one of ( g, r, i, z, y )
            POS: CIRCLE <longitude> <latitude> <radius>
                longitude (ra) : `float`
                latitude(dec): `float`
                radius: `float`

        Returns
        -------
        image : `afw_image`

        """
        cutout = get_image(params, self._config)
        return cutout

    def get_range(self, params: dict) -> afw_image:
        """ Method to retrieve image cutout specified in RANGE.

        All longitude(ra) and latitude(dec) values in ICRS degrees.

        Parameters
        ----------
        params: 'dict'
            ID: <db>.<ds>.<filter>
                db: database identifier
                ds: dataset identifier
                filter: one of ( g, r, i, z, y )
            POS: RANGE <longitude1> <longitude2> <latitude1> <latitude2>
                longitude1(ra1) : `float`
                longitude2(ra2): `float`
                latitude1(dec1): `float`
                latitude2(dec2): `float`

        Returns
        -------
        image : `afw_image`

        """
        cutout = get_image(params, self._config)
        return cutout

    def get_polygon(self, params: dict) -> afw_image:
        """ Method to retrieve image cutout specified in POLYGON.

        All contained ra and dec values in ICRS degrees.

        Parameters
        ----------
        params: 'dict'
            ID: <db>.<ds>.<filter>
                db: database identifier
                ds: dataset identifier
                filter: one of ( g, r, i, z, y )
            POS: POLYGON <longitude1> <latitude1> ... (at least 3 pairs)
                longitude1: 'float'
                latitude1: 'float'

        Returns
        -------
        image : `afw_image`

        """
        cutout = get_image(params, self._config)
        return cutout

    def get_brect(self, params: dict) -> afw_image:
        """ LSST Extension to SODA: retrieve image cutout specified in BRECT.

        Parameters
        ----------
        params: `dict`
            ID: <db>.<ds>.<filter>
                db: database identifier
                ds: dataset identifier
                filter: one of ( g, r, i, z, y )
            POS: BRECT <ra> <dec> <width> <height> <unit>
                ra : `float`
                    the longitude.
                dec : `float`
                    the latitude.
                width: 'int'
                    size of width.
                height: `int`
                    size of height.
                unit: pixel, arcsec
                    size in pixel or angular unit.

        Returns
        -------
        image : afw_image

        """
        cutout = get_image(params, self._config)
        return cutout
