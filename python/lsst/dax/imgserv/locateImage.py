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

import lsst.daf.base as dafBase
import lsst.log as log
from lsst.obs.sdss import sdssMapper

from .getimage.imagegetter_v1 import ImageGetter_v1
from .butlerGet import  ButlerGet
from .metaservGet import MetaservGet

def image_open_v1(W13db, config, logger=log):
    """Open access to specified images (raw, calexp,
    deepCoadd,etc) of specified image repository.

    Returns
    -------
    imagegetter : obj
        instance for access to all image operations.

    """
    imagedb = W13db(config, logger)
    return ImageGetter_v1(imagedb.butlerget, imagedb.metaservget, logger)


class W13Db:
    """This is the base class for examining DC_W13_Stripe82 image data,
    Thie instantates a Butler for access to image repository, as well as
    connection for metadata via MetaServ.

    Attributes
    ----------
    imagegetter : obj
        To be used for accessing images.
    """

    def __init__(self, config, table, columns, dataRoot,
            butlerPolicy, butlerKeys, logger):
        """Instantiate W13Db object with credential for database, butler
        configuration, and logger.

        Parameters
        ----------
        config: Dict
            configuration file for this imgserv instance.
        table : str
            The table name.
        columns : str
            The database columns.
        dataRoot : str
            root for the butler.
        bulterPolicy : str
            The butler policy.
        butlerKeys : str
                     The butler keys for this image data source.
        logger : obj
            The logger to be used.

        """
        self.butlerget = ButlerGet(dataRoot, butlerPolicy, butlerKeys, logger)
        self.metaservget = MetaservGet(
                config["DAX_IMG_META_URL"],
                config["DAX_IMG_META_DB"],
                table, columns, logger)

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
    def __init__(self, config, logger=log):
        """Instantiate W13RawDb object with DB credential info and logger."""
        W13Db.__init__(self,
                       config,
                       table=config["DAX_IMG_TAB_SCICCDEXP"],
                       columns=config["DAX_IMG_COLUMNS1"],
                       dataRoot=config["DAX_IMG_DR"],
                       butlerPolicy=config["DAX_IMG_BUTLER_POL0"],
                       butlerKeys=config["DAX_IMG_BUTLER_KEYS1"],
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
    def __init__(self, config, logger=log):
        """Instantiate W13CalexpDb object with DB credential info and
        logger."""
        W13Db.__init__(self,
                       config,
                       table=config["DAX_IMG_TAB_SCICCDEXP"],
                       columns=config["DAX_IMG_COLUMNS1"],
                       dataRoot=config["DAX_IMG_DS"]+"/calexps",
                       butlerPolicy=config["DAX_IMG_BUTLER_POL1"],
                       butlerKeys=config["DAX_IMG_BUTLER_KEYS1"],
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
    def __init__(self, config, logger=log):
        """Instantiate W13DeepCoaddDb object with DB credential and logger."""
        W13Db.__init__(self,
                       config,
                       table=config["DAX_IMG_TAB_DEEPCOADD"],
                       columns=config["DAX_IMG_COLUMNS2"],
                       dataRoot=config["DAX_IMG_DS"]+"/coadd",
                       butlerPolicy=config["DAX_IMG_BUTLER_POL2"],
                       butlerKeys=config["DAX_IMG_BUTLER_KEYS2"],
                       logger=logger)

