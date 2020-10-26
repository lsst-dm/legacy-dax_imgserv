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
This module is used to instantiate or fetch from cache the
appropriate butler instance.

"""
from flask import current_app, has_app_context

import lsst.log as log
# Gen3
from lsst.daf.butler import Butler

import etc.imgserv.imgserv_config as imgserv_config


class ButlerGet(object):
    """Instantiate and hold onto instance of Butler for ImageGetter.
    """

    _butler_instances = {}  # caching butler instances for CLI context only

    def __init__(self, butler, ds, dataset_type, dataid_keys):
        """Instantiate ButlerGet per specification.
        """
        self.butler = butler
        self.dataset_config = ds
        self.butler_ds = dataset_type
        self.butler_keys = dataid_keys

    @staticmethod
    def get_butler(ds, datarepo_id, ds_type, dataid_keys):
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
            log.debug(f"Instantiating Butler Gen3 with data repository: {datarepo_id}")
            if ds == "default":
                ds = imgserv_config.config_datasets["default"]
            collection = imgserv_config.config_datasets[ds]["IMG_DEFAULT_COLLECTION"]
            butler = Butler(datarepo_id, collections=collection)
            butler_instances[datarepo_id] = butler
        return ButlerGet(butler, ds, ds_type, dataid_keys)
