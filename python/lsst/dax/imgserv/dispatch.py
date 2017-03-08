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

from .image import Image
from .hashutil import Hasher

class Dispatcher(object):
    """ Dispatcher maps request to corresponding Image method. 
    """

    def __init__(self, config_dir):
        """ Load and keep ref to the key to API map """
        config = os.path.join(config_dir, "request_to_api.json")
        with open(config) as jason_api:
            self.api_map = json.load(jason_api)  
            jason_api.close()

    def find_api(self, request):
        """ Find the API based on signature.
            for example, ['raw', 'nearest']
        Parameters
        ----------
        request: 
            the request object.
        
        Returns
        -------
            the matching API method of the Image class.
            parameters for the API method.
        """
        keys = request["get"]["api_id"]    
        sig = Hasher.hash(str(keys).encode())
        entry = self.api_map[sig.hexdigest()]
        if entry is not None:
            api_str = entry["api"]
            # example for api_str: 'Image.cutout'
            mod_name, func_name = api_str.split(".")
            assert(mod_name == "Image")
            api = getattr(Image, func_name)
            if api == None:
                return None, None
            # fetch the parameters specified by keys 
            params = self.get_params(request, keys)
            return api, params

    def get_params(self, req, keys):
        """ Get the parameters corresponding to the API """
        image = req["get"]["image"]
        p_list = self._flatten_json(image)
        # params to be list of all items related to keys
        params = {}
        for k in keys:
            for p in p_list:
                if k in p:
                    params[p]=p_list[p] # copy it
        return params

    def _key_in_param(self, key, param):
        p_items = param.split(".")
        if p_items[-1] == key:
            return True
        else:
            return False

    def _flatten_json(self, req):
        """ flatten request elements into a dictionary. """
        p_elems = {}

        def flatten(r, name=""):
            if isinstance(r, dict):
                for x in r:
                    flatten(r[x], name+x+".")
            elif isinstance(r, list):
                p_elems[name[:-1]].append(r)
            else:
                p_elems[name[:-1]] = r

        flatten(req)
        return p_elems

