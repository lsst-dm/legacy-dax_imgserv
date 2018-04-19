#
# LSST Data Management System
# Copyright 2018, 2017 LSST/AURA.
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
#
#
# This code is used to to select an image or a cutout of an image
# that has its center closest to the specified RA and Dec. The
# image is retrieved using the Data Butler.

"""
This module is used to fetch metadata based on astronomical parameters.

@author: John Gates, SLAC
@author: Brian Van Klaveren, SLAC
@author: Kenny Lo, SLAC

"""

class MetaservGet:
    """Class to fetch image metadata based on astronomical parameters.

    """

    def  __init__(self, conn, table, columns, logger):
        """Instantiate MetaServGet for access to image medatadata.

        Parameters
        ----------
        conn :
                the connection to database server.
        columns :
                the database columns.
        logger: obj
                used for logging messages.
        """
        self._log = logger
        self._conn = conn
        self._table = table
        self._columns = columns

    def nearest_image_containing(self, ra, dec, filtername):
        """Find nearest image containing the [ra, dec].

        Parameters
        ----------

        ra : degree
        dec : degree
        filtername: str [optional]

        Returns
        -------
        qResults: dict
            the result of the SQL query.
        """
        cols = ["ra", "decl"]
        for s in self._columns:
            cols.append(s)
        dist = "(power((ra - {}),2) + power((decl - {}),2)) as distance".format(ra, dec)
        # More accurate distance calc on a sphere-if needed
        # SELECT *, 2 * ASIN(SQRT(POWER(SIN((raA)*pi()/180/2),2)+
        # COS(raA*pi()/180)*COS(abs(raB)*pi()/180)*
        # POWER(SIN((decB.lon)*pi()/180/2),2)) as distance
        # FROM <table> order by distance ;
        filterSql = ""
        if filtername:
            filterSql = "filtername = '{}' AND".format(filtername)
        cols.append(dist)
        col_str = ",".join(cols)
        sql = ("SELECT {} FROM {} WHERE {} "
               "scisql_s2PtInCPoly({}, {}, "
               "corner1Ra, corner1Decl, corner2Ra, corner2Decl, "
               "corner3Ra, corner3Decl, corner4Ra, corner4Decl) = 1 "
               "order by distance LIMIT 1").format(col_str, self._table, filterSql, ra, dec)
        self._log.debug("findNearest sql={}".format(sql))
        r = self._conn.execute(sql).fetchall()
        return r



