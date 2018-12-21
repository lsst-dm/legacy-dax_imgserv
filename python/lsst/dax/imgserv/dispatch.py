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

"""

import os
import json

from .image import Image
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
        if req_params["API"]== "SODA":
            api_params = self._map_soda_params(req_params)
        else: # default
            api_params = self._map_url_params(req_params)
        ids = sorted(api_params.keys())
        api_id = Hasher.hash(str(ids).encode("utf-8")).hexdigest()
        entry = self.api_map[api_id]
        if entry:
            mod_func = entry["api"]
            # example for api_str: 'Image.cutout'
            mod_name, func_name = mod_func.split(".")
            api = getattr(Image, func_name)
            return api, api_params

    @staticmethod
    def _map_url_params(req_params):
        api_params = {}
        # map ra,dec into center.x,center.y
        ra = req_params["ra"]
        if ra:
            api_params["center.x"] = ra
        dec = req_params["dec"]
        if dec:
            api_params["center.y"] = dec
        if ra or dec:
            api_params["center.unit"] = "deg"
            filt = None
            if "run" in req_params or "tract" in req_params:
                # check for data id before renaming filter
                pass
            else:
                filt = req_params["filter"]
            if filt:
                api_params["filter"] = filt
        sid = req_params["sid"]
        if sid:
            api_params["science_id"] = sid
        width = req_params["width"]
        if width:
            api_params["size.x"] = width
        height = req_params["height"]
        if height:
            api_params["size.y"] = height
        unit = req_params["unit"]
        if unit:
            api_params["size.unit"] = unit
        patch = req_params["patch"]
        if patch:
            patch_x, patch_y = patch.split(",")
            api_params["patch_x"] = patch_x
            api_params["patch_y"] = patch_y
        return api_params

    @staticmethod
    def _map_soda_params(req_params):
        box = req_params["POS"]
        shape, ra, dec, w, h, unit = box.split()
        filt = req_params["filter"]
        api_params = {
            "db": req_params["db"],
            "ds": req_params["ds"],
            "center.x": ra,
            "center.y": dec,
            "center.unit":  "deg",
            "filter": filt,
            "size.x": w,
            "size.y": h,
            "size.unit": unit
        }
        return api_params

