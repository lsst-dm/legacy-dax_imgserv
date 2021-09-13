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
This module defines the abstract base class for implementing SODA, per IVOA
specification:
    http://www.ivoa.net/documents/SODA/20170517/REC-SODA-1.0.htm
l
All ra,dec values are expressed in ICRS degrees.
"""
from .dal import DAL

from abc import abstractmethod
from typing import TypeVar


"""
Shape:
    CIRCLE <ra> <dec> <radius>
    RANGE <ra1> <ra2> <dec1> <dec2>
    POLYGON <ra1> <dec1> ... (at least 3 pairs)
"""

Param_Datatype = {
    "Name": [ "UCD", "Unit", "Semantics" ],
    "ID": [ "meta.ref.url;meta.curation", "", "cf. sect. 3.2.1" ],
    "CIRCLE": [ "pos.outline;obs", "deg", "cf. sect. 3.3.2" ],
    "POLYGON": [ "pos.outline;obs", "deg", "cf. sect. 3.3.3" ],
    "POS": [ "pos.outline;obs", "", "cf. sect. 3.3.1" ],
    "BAND": [ "em.wl;stat.interval", "m", "cf. sect. 3.3.4" ],
    "TIME": [ "time.interval;obs.exposure", "d", "cf. sect. 3.3.5" ],
    "POL": [ "meta.code;phys.polarization", "", "cf. sect. 3.3.6" ]
}

""" Note: values to be escaped by '+' """

img = TypeVar('img', bound=object)


class SODA(DAL):
    """ Interface defined for SODA to extract image cutouts.
    """
    def handle_pos(self, params: dict) -> img:
        """"
        Parameters
        ----------
        params: `dict`
            the request parameters.

        Returns
        -------
        img : `object`
            the image object.
        """
        return self.handle_default(params)

    @abstractmethod
    def handle_default(self, params: dict) -> img:
        """ Default handler: To be implemented by subclass

        Parameters
        ----------
        params: `dict`
            the request parameters.

        Returns
        -------
        img : `object`
            the image object.
        """
        pass

    @abstractmethod
    def get_circle(self, params: dict) -> img:
        """ Implement this method in subclass to retrieve image cutout
        specified in CIRCLE.

        All input values in ICRS degrees, including the radius.

        Parameters
        ----------
        params: `dict`
            POS: CIRCLE <longitude> <latitude> <radius>
                longitude (ra) : `float`
                latitude(dec): `float`
                radius: `float`

        Returns
        -------
        img : `object`
            the image object.

        """
        pass

    @abstractmethod
    def get_range(self, params: dict) -> img:
        """ Implmement this method to in subclass to retrieve image cutout
        specified in RANGE.

        All longitude(ra) and latitude(dec) values in ICRS degrees.

        Parameters
        ----------
        params: 'dict'
            POS: RANGE <longitude1> <longitude2> <latitude1> <latitude2>
                longitude1(ra1) : `float`
                longitude2(ra2): `float`
                latitude1(dec1): `float`
                latitude2(dec2): `float`

        Returns
        -------
        img : `object`
            the image object.
        """
        pass

    @abstractmethod
    def get_polygon(self, params: dict) -> img:
        """ Implement this method in subclass to retrieve image cutout
            specified in RANGE.

        All longitude(ra) and latitude(dec) values in ICRS degrees.

        Parameters
        ----------
        params: 'dict'
            POS: POLYGON <longitude1> <latitude1> ... (at least 3 pairs)
                longitude1: 'float'
                latitude1: 'float'

        Returns
        -------
        img : `object`
            the image object.

        """
        pass
