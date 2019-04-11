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

    def find_api(self, req_params):
        """ Find the API based on its method signature.

        Parameters
        ----------
        req_params: `dict`
            the parameters with values.

        Returns
        -------
        api, api_param: `Image.func_name`, `dict`
            the matching API of Image class and the parameters.
        """
        if req_params.get("API") == "SODA":
            api_params = self._map_soda_params(req_params)
        else:  # v1 (legacy)
            api_params = self._map_url_params(req_params)
        ids = sorted(api_params.keys())
        api_id = Hasher.md5(ids)
        entry = self.api_map.get(api_id)
        if entry:
            mod_func = entry["api"]
            # example for api_str: 'Image.cutout'
            mod_name, func_name = mod_func.split(".")
            api = getattr(Image, func_name)
            return api, api_params
        else:
            raise Exception("Dispatcher.find_api(): API not found")

    @staticmethod
    def _map_url_params(req_params):
        """" Extract and map API parameters from the request. The return
        parameters should match that of  the API signature map.
        """
        api_params = req_params.copy()
        if api_params.get("API"):
            api_params.pop("API")  # not needed for API signature
        # deg presumed for center coordinates
        if api_params.get("center.unit"):
            api_params.pop("center.unit")
        # map ra,dec into center.x,center.y
        ra = api_params.pop("ra", None)
        if ra:
            api_params["center.x"] = ra
        dec = api_params.pop("dec", None)
        if dec:
            api_params["center.y"] = dec
        if ra or dec:
            filt = None
            if "run" in api_params or "tract" in api_params:
                # case of filter name already specified in data id
                pass
            else:
                filt = api_params.get("filter")
            if filt:
                api_params["filter"] = filt
        width = api_params.pop("width", None)
        if width:
            api_params["size.x"] = width
        height = api_params.pop("height", None)
        if height:
            api_params["size.y"] = height
        unit = api_params.pop("unit", None)
        if unit:
            api_params["size.unit"] = unit
        patch = api_params.pop("patch", None)
        if patch:
            patch_x, patch_y = patch.split(",")
            api_params["patch_x"] = patch_x
            api_params["patch_y"] = patch_y
        return api_params

    @staticmethod
    def _map_soda_params(req_params):
        """ Map the SODA parameters from the request.
        """
        db, ds, filt = req_params["ID"].split(".")
        api_params = {
            "db": db,
            "ds": ds,
            "filter": filt,
            "ID": req_params["ID"],
            "POS": req_params["POS"]
        }
        return api_params

