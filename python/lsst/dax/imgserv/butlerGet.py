#
# LSST Data Management System
# Copyright 2017 LSST/AURA.
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
This module is used to instantiate or fetch from cache the
appropriate butler instance.

@author: John Gates, SLAC
@author: Brian Van Klaveren, SLAC
@author: Kenny Lo, SLAC

"""
from flask import current_app, has_app_context
import lsst.daf.persistence as dafPersist


class ButlerGet(object):
    """Instantiate and hold onto instance of Butler for ImageGetter.
    """

    _butler_instances = {}  # caching butler instances for CLI context only

    def __init__(self, dataRoot, butler_policy, butler_keys, logger):
        """Instantiate ButlerGet per specification.
        """
        self.logger = logger
        logger.debug("Instantiating ButlerGet with dataRoot: {}".format(dataRoot))
        self.butler = self.get_butler(datarepo_id=dataRoot, logger=logger)
        self.butler_policy = butler_policy
        self.butler_keys = butler_keys

    def get_butler(cls, datarepo_id, logger):
        """Get butler instance from cache if available and instantiate if not.
        """
        if has_app_context():
            # flask application context
            butler_instances = current_app.butler_instances
        else:
            # CLI context
            butler_instances = ButlerGet._butler_instances
        butler = butler_instances.get(datarepo_id)
        if not butler:
            # new butler instance needed
            logger.debug("Instantiating new butler with data repository:\
                    {}".format(datarepo_id))
            butler = dafPersist.Butler(inputs=datarepo_id)
            butler_instances[datarepo_id] = butler
        return butler

