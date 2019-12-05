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
This module wraps the standard hash functions to generate message
digests.
"""

import hashlib


class Hasher(object):
    """ Hasher is wrapper to hash functions
    """

    ""
    @classmethod
    def md5(cls, data):
        """ Function to return the message digest with MD5.

        Appropriate for use with non-science data.

        Parameters
        ----------
        data : 'Iterable'

        Returns
        -------
        hash : `str`

        """
        if isinstance(data, (bytes, bytearray)):
            return hashlib.md5(data).hexdigest()
        else:
            return hashlib.md5(str(data).encode("utf-8")).hexdigest()

    ""
    @classmethod
    def sha256(cls, data):
        """ Function to return the message digest with sha256.

        Recommended for usage with scientific data.

        Parameters
        ----------
        data : 'Iterable'

        Returns
        -------
        hash : `str`

        """
        if isinstance(data, (bytes, bytearray)):
            return hashlib.sha256(data).hexdigest()
        else:
            return hashlib.sha256(str(data).encode("utf-8")).hexdigest()
