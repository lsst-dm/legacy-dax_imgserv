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

# This code is used to to select an image or a cutout of an image
# that has its center closest to the specified RA and Dec. The
# image is retrieved using the Data Butler.

"""
This module is used to fetch metadata based on astronomical parameters.

"""
import re
from sqlalchemy import create_engine

import pyvo as vo
import etc.imgserv.imgserv_config as imgserv_config


class MetaGet:
    """Class to fetch image metadata based on astronomical parameters.

    """

    def __init__(self, ds, config):
        """Instantiate MetaServGet for access to image medatadata.

        Parameters
        ----------
        ds: `str`
            the dataset identifier.
        config: `dict`
                the configuration file.
        """
        self._config = config
        image_meta_url = config.get("DAX_IMG_META_URL", "")
        dataset = imgserv_config.config_datasets.get(ds, None)
        db_url = image_meta_url + "/" + dataset["IMG_OBSCORE_DB"]
        # TODO: Need to test against ObsTAP server
        self._obstap_service = vo.dal.TAPService(db_url)
        self._engine = create_engine(db_url)

    def adql_nearest_image_contains(self, ra, dec, radius):
        """ Find nearest image containing Circle(ra, dec, radius) from ObsTAP server.

        Parameters
        ----------
        ra: `float`
        dec: `float`
        radius:`float`

        Returns
        -------
        result: dict
                the result of the SQL query.
        """
        obscore_table = self._config["IMG_SCHEMA_TABLE"]
        adql = f"SELECT * from {obscore_table} where CONTAINS(CIRCLE({ra}, {dec}," \
               f"{radius}), s_region)=1"
        rs = self._obstap_service.search(adql)
        return rs.votable

    def pg_nearest_image_contains(self, ds_type, ra, dec, radius, f_name):
        """ Find nearest image containing Circle(ra, dec, radius) from Postgres DB.

        Parameters
        ----------
        ds_type: 'str'
            the dataset type.
        ra: `float`
        dec: `float`
        radius:`float`
        f_name: `str`
            the filter name.

        Returns
        -------
        result: dict
                the result of the SQL query.
        """
        obscore_table = self._config["IMG_SCHEMA_TABLE"]
        psql = f"SELECT * FROM {obscore_table} WHERE " \
               f"dataproduct_subtype='lsst.{ds_type}' AND em_filter_name='{f_name}' AND " \
               f"position_bounds_spoly ~ scircle(spoint(radians({ra}), " \
               f"radians({dec})), radians({radius}))"
        result = self._engine.execute(psql).fetchall()
        return result

    def nearest_image_contains(self, ds_type, ra, dec, radius, f_name):
        """Find nearest image containing the [ra, dec] of radius and filter name.

        Parameters
        ----------

        ds_type : str
            the dataset type.
        ra : degree
        dec : degree
        radius : arcsec
        f_name: str
            the filter name.
        Returns
        -------
        r: [(...),(...),...]
            the result of the SQL query.
        """
        if f_name is None:
            f_name = self._config["IMG_DEFAULT_FILTER"]
        return self.pg_nearest_image_contains(ds_type, ra, dec, radius, f_name)

    def adql2pg_nearest_image_contains(self, params: dict) -> object:
        """ Execute arbitrary ADQL queries via ObsTAP.

        Parameters
        ----------
        params: `dict`

        Returns
        --------
        response: `str`

        """
        query = params["adql"]
        m = re.match("SELECT * from (.*) where CONTAINSS(CIRCLE(("
                     ".*), (.*), (.*)), s_region)=1", query)
        obscore_table = str(m.group(1))
        ra = float(m.group(2))
        dec = float(m.group(3))
        radius = float(m.group(4))
        psql = f"SELECT * FROM {obscore_table} WHERE " \
               f"position_bounds_spoly ~ scircle(spoint(RADIANS({ra}, " \
               f"RADIANS({dec}), RADIANS({radius}))"
        rs = self._obstap_service.search(psql)
        return rs.votable
