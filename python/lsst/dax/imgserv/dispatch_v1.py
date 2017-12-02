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

"""
This module implements the API dispatch logic.

@author: Kenny Lo, SLAC

"""

import os
import json

from .image_v1 import Image
from .hashutil import Hasher


class Dispatcher(object):
    """ Dispatcher maps request to corresponding Image method.
    """

    def __init__(self, config_dir):
        """Load and keep ref to the key to API Map."""
        config = os.path.join(config_dir, "api_map.json")
        with open(config) as jason_api:
            self.api_map = json.load(jason_api)
            jason_api.close()

    def find_api(self, req_params):
        """ Find the API based on its ID.

        Parameters
        ----------
        req_params: dict
            the paramters with values.

        Returns
        -------
        api: function
            the matching API method of the Image class.
        """
        self._map_url_params(req_params)
        ids = sorted(req_params.keys())
        api_id = Hasher.hash(str(ids).encode("utf-8")).hexdigest()
        entry = self.api_map[api_id]
        if entry:
            mod_func = entry["api"]
            # example for api_str: 'Image.cutout'
            mod_name, func_name = mod_func.split(".")
            api = getattr(Image, func_name)
            return api

    def _map_url_params(self, req_params):
        # map ra,dec into cente.x,center.y
        ra = req_params.pop("ra", None)
        if ra:
            req_params["center.x"] = ra
        dec = req_params.pop("dec", None)
        if dec:
            req_params["center.y"] = dec
        if ra or dec:
            req_params["center.unit"] = "deg"
            filt = None
            if "run" in req_params or "tract" in req_params:
                # check for data id before renaming filter
                pass
            else:
                filt = req_params.pop("filter", None)
            if filt:
                req_params["filter"] = filt
        sid = req_params.pop("sid", None)
        if sid:
            req_params["science_id"] = sid
        width = req_params.pop("width", None)
        if width:
            req_params["size.x"] = width
        height = req_params.pop("height", None)
        if height:
            req_params["size.y"] = height
        unit = req_params.pop("unit", None)
        if unit:
            req_params["size.unit"] = unit
        patch = req_params.pop("patch", None)
        if patch:
            patch_x, patch_y = patch.split(",")
            req_params["patch_x"] = patch_x
            req_params["patch_y"] = patch_y

