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

from datetime import datetime

from flask import session

import lsst.afw.image as afw_image

from ..locateImage import get_image
from .soda.soda import SODA
# use following format for circular reference
import lsst.dax.imgserv.jobqueue.imageworker as imageworker

""" This module implements the IVOA's SODA v1.0 per LSST requirements.

    All ra,dec values are expressed in ICRS degrees, by default.

    Shape parameters:
        CIRCLE <ra> <dec> <radius>
        RANGE <ra1> <ra2> <dec1> <dec2>
        POLYGON <ra1> <dec1> ... (at least 3 pairs)
        BRECT <ra> <dec> <w> <h> <filter> <unit>
       
    The parameters are represented in `dict`, for example: 
        {'ID': 'DC_W13_Stripe82.calexp.r', 'CIRCLE 37.644598 0.104625 100'}
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

    def do_sync(self, params: dict) -> afw_image:
        """ Do sync operation.

        Parameters
        ---------
        params : `dict`
            the request parameters (See Shape requirements above)

        Returns
        -------
        resp: `lsst.afw.image`
            the image object.
        """
        if "POS" in params:
            resp = super().handle_pos(params)
            if isinstance(resp, tuple):
                # image object is first element of the tuple
                return resp[0]
            else:
                return resp
        else:
            raise NotImplementedError("ImageSODA.do_sync(): Unsupported "
                                      "Request")

    def do_async(self, params: dict) -> str:
        """ For async operation, create a new task for the request, enqueue for
        later processing, then return the task_id for tracking it.

        Parameters
        ----------
        params : `dict`
            the request parameters. (See Shape parameters above)

        Returns
        -------
        task.task_id: `str`
            the newly created task/job id.

        """
        user = session.get("user", "UNKNOWN")
        # enqueue the request for image_worker
        job_start_time = datetime.timestamp(datetime.now())
        # task = imageworker.get_image_async.delay(job_start_time, params)
        kwargs = {"job_start_time": job_start_time, "owner": user}
        task = imageworker.get_image_async.apply_async(
            queue="imageworker_queue",
            args=[params],
            kwargs=kwargs
        )
        return task.task_id

    def do_sia(self, params: dict) -> object:
        """ Do async operation.

        Parameters
        ----------
        params : `dict`
            the request parameters.

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
        cutout : `lsst.afw.image`

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
        cutout : `lsst.afw.image`

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
        image : `lsst.afw.image`

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
        cutout : `lsst.afw.image`
i
        """
        cutout = get_image(params, self._config)
        return cutout
