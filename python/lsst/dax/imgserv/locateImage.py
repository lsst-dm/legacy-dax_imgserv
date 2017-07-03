#
# LSST Data Management System
# Copyright 2015 LSST/AURA.
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
This module is used to locate and retrieve various images
(e.g. raw, calexp, deepCoadd), and their related metadata.

@author: John Gates, SLAC
@author: Brian Van Klaveren, SLAC
@author: Kenny Lo, SLAC

"""

import gzip
import math
import os
import sys
import time

from flask import current_app

from sqlalchemy.exc import SQLAlchemyError

import lsst.daf.base as dafBase
import lsst.daf.persistence as dafPersist
import lsst.log as log
from lsst.db.engineFactory import getEngineFromFile
from lsst.obs.sdss import sdssMapper

from .getimage.imagegetter import ImageGetter


def image_open(credFileName, W13db, logger=log):
    """Open access to specified images (raw, calexp,
    deepCoadd,etc) of specified image repository.

    Returns
    -------
    imagegetter : obj 
        instance for access to all image operations.
                
    """
    imagedb = W13db(credFileName)
    return imagedb.imagegetter


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
               "scisql_s2PtInBox({}, {}, corner1Ra, corner1Decl, corner3Ra, corner3Decl) = 1 "
               "order by distance LIMIT 1").format(col_str, self._table, filterSql, ra, dec)
        self._log.info(sql)
        self._log.info("findNearest sql={}".format(sql))
        return self._conn.execute(sql).fetchall()


class ButlerGet:
    """Class to instantiate and hold instance of Butler for ImageGetter.
    
    """

    def __init__(self, dataRoot, butler_policy, butler_keys, logger):
        """Instantiate ButlerGet to be passed to ImageGetter."""
        self.butler = dafPersist.Butler(dataRoot)
        self.butler_policy = butler_policy
        self.butler_keys = butler_keys
        logger.debug("Instantiate ButlerGet.")


class W13Db:
    """This is the base class for examining DC_W13_Stripe82 image data,
    Thie instantates a Butler for access to image repository, as well as
    connection for metadata via MetaServ.

    Attributes
    ----------
    imagegetter : obj
        To be used for accessing images.
    """

    def __init__(self, credFileName, database, table, columns, dataRoot, 
            butlerPolicy, butlerKeys, logger):
        """Instantiate W13Db object with credential for database, butler 
        configuration, and logger.
    
        Parameters
        ----------
        credFileName : str
            The connection for accessing image metadata
        database : str
            the datbase connection string.
        table : str
            The table name.
        columns : str
            The database columns.
        dataRoot : str
            root for the butler.
        bulterPolicy : str
            The butler policy.
        butlerKeys : str
                      The bulter keys for this image data source.
        logger : obj
            The logger to be used.

        """
        self._log = logger
        self.conn = getEngineFromFile(credFileName, database=database).connect()
        self.butlerget = ButlerGet(dataRoot, butlerPolicy, butlerKeys, logger)
        self.metaservget = MetaservGet(self.conn, table, columns, logger)
        self.imagegetter = ImageGetter(self.butlerget, self.metaservget, logger)
        try:
            sql = "SET time_zone = '+0:00'"
            self._log.info(sql)
            self.conn.execute(sql)
        except SQLAlchemyError as e:
            self._log.error("Db engine error %s" % e)


class W13RawDb(W13Db):
    """This class is used to connect to the DC_W13_Stripe82 Raw database.
    Raw images
    ----------
    Repository path: /datasets/sdss/preprocessed/dr7/runs
    Butler keys: run, camcol, field, filter
    MySQL table: DC_W13_Stripe82.Science_Ccd_Exposure
    Table columns: run, camcol, field, filterName
    butler.get("raw", run=run, camcol=camcol, field=field, filter=filterName)

    """

    def __init__(self, credFileName, logger=log):
        """Instantiate W13RawDb object with DB credential info and logger."""
        W13Db.__init__(self,
                       credFileName,
                       current_app.config["DAX_IMG_DB"],
                       current_app.config["DAX_IMG_TAB_SCICCDEXP"],
                       columns=current_app.config["DAX_IMG_COLUMNS1"],
                       dataRoot=current_app.config["DAX_IMG_DR"]+"/runs",
                       butlerPolicy=current_app.config["DAX_IMG_BUTLER_POL0"],
                       butlerKeys=current_app.config["DAX_IMG_BUTLER_KEYS1"],
                       logger=logger)


class W13CalexpDb(W13RawDb):
    """This class is used to connect to the DC_W13_Stripe82 Calibration Exposures.
    Calibration Exposures look to be very similar to retrieving Raw exposres. Once
    this is shown to work, W13CalebDb and W13RawDb should be refactored to have a
    commnon base class and add a field for policy "fpC" or "calexp".
    ----------
    Repository path: /datasets/sdss/preprocessed/dr7/sdss_stripe82_00/calexps/
    Butler keys: run, camcol, field, filter
    MySQL table: DC_W13_Stripe82.Science_Ccd_Exposure
    Table columns: run, camcol, field, filterName
    butler.get("calexp", run=run, camcol=camcol, field=field, filter=filterName)

    """
    
    def __init__(self, credFileName, logger=log):
        """Instantiate W13CalexpDb object with DB credential info and
        logger."""
        W13Db.__init__(self,
                       credFileName,
                       database=current_app.config["DAX_IMG_DB"],
                       table=current_app.config["DAX_IMG_TAB_SCICCDEXP"],
                       columns=current_app.config["DAX_IMG_COLUMNS1"],
                       dataRoot=current_app.config["DAX_IMG_DS"]+"/calexps",
                       butlerPolicy=current_app.config["DAX_IMG_BUTLER_POL1"],
                       butlerKeys=current_app.config["DAX_IMG_BUTLER_KEYS1"],
                       logger=logger)


class W13DeepCoaddDb(W13Db):
    """This class is used to connect to the DC_W13_Stripe82 Coadd database.
    Coadd images
    ------------
    Repository path: "/datasets/sdss/preprocessed/dr7/sdss_stripe82_00/coadd"
    Butler keys: tract, patch, filter
    MySQL table: DC_W13_Stripe82.DeepCoadd
    Table columns: tract, patch, filterName
    butler.get("deepCoadd", filter=filterName, tract=tract, patch=patch)
    
    """
    
    def __init__(self, credFileName, logger=log):
        """Instantiate W13DeepCoaddDb object with DB credential and logger."""
        W13Db.__init__(self,
                       credFileName,
                       database=current_app.config["DAX_IMG_DB"],
                       table=current_app.config["DAX_IMG_TAB_DEEPCOADD"],
                       columns=current_app.config["DAX_IMG_COLUMNS2"],
                       dataRoot=current_app.config["DAX_IMG_DS"]+"/coadd",
                       butlerPolicy=current_app.config["DAX_IMG_BUTLER_POL2"],
                       butlerKeys=current_app.config["DAX_IMG_BUTLER_KEYS2"],
                       logger=logger)

