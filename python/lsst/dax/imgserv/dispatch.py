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

from .exceptions import UsageError

from lsst.dax.imgserv.image import Image

import etc.imgserv.imgserv_config as imgserv_config


class Dispatcher(object):
    """ Dispatcher maps request to corresponding Image method.
    """

    def __init__(self, config_dir):
        """Load and keep ref to the key to API Map."""
        config = os.path.join(config_dir, "api_map.json")
        self.api_map = {}
        with open(config) as f:
            apis = json.load(f)
            for key in apis.keys():
                s_key = ",".join(sorted(key.split(",")))
                self.api_map[s_key] = apis[key]

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
        api_params = self._map_soda_params(req_params)
        api_id = self._get_api_id(api_params)
        module_func = self.api_map.get(api_id)
        if module_func:
            api = eval(module_func)
            return api, api_params
        else:
            raise Exception("Dispatcher: API method not Found")

    @staticmethod
    def _get_api_id(api_params):
        params_l = list(api_params.keys())
        params_l.remove("ds")
        params_l.remove("dsType")
        params_l.remove("filter")
        api_id = ",".join(sorted(params_l))
        return api_id

    @staticmethod
    def _map_soda_params(req):
        """ Map the SODA parameters from the request.
        """
        ds, ds_type, filt = req["ID"].split(".")
        pos = req.get("POS", None)
        api_params = {"ds": ds}
        if imgserv_config.config_datasets.get(ds, None) is not None:
            api_params["dsType"] = ds_type
            api_params["filter"] = filt
            if pos == "NA" or pos is None:
                if ds_type == "calexp":
                    api_params["visit"] = int(req["visit"])
                    api_params["detector"] = int(req["detector"])
                    api_params["instrument"] = req["instrument"]
                elif ds_type == "deepCoadd":
                    api_params["band"] = req["band"]
                    api_params["skymap"] = req["skymap"]
                    api_params["tract"] = int(req["tract"])
                    api_params["patch"] = int(req["patch"])
                elif ds_type == "raw":
                    api_params["instrument"] = req["instrument"]
                    api_params["detector"] = int(req["detector"])
                    api_params["exposure"] = int(req["exposure"])
                else:
                    raise UsageError("Missing POS or data id in request")
            else:
                api_params["POS"] = pos
        else:
            raise UsageError("Unrecognized dataset identifier")
        return api_params
