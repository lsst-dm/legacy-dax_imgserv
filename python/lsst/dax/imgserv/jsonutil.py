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
This module implements various JSON-related utility functions and classes.

@author: Kenny Lo, SLAC

"""

def flatten_json(j):
    """ Flatten JSON object into a dictionary. """
    j_d = {}

    def flatten(r, name=""):
        if isinstance(r, dict):
            for x in r:
                flatten(r[x], name+x+".")
        elif isinstance(r, list):
            j_d[name[:-1]].append(r)
        else:
            j_d[name[:-1]] = r

    flatten(j)
    return j_d


def _endswith(param, k):
    if param.endswith("."+k):
        return True
    elif k == param:
        return True
    else:
        return False


def get_params(req):
    """ Get the parameters corresponding to the API.
    The extraction of each parameter is based upon best match
    of the name with parts delimited by '.'.

    Parameters
    ----------
    req: the request in JSON

    Returns
    -------
    dict
        the list of parameters and their values.
    """
    keys = req["api_id"]
    image = req["image"]
    p_list = flatten_json(image)
    # params to be list of all items related to keys
    params = {}
    for k in keys:
        for p in p_list:
            if _endswith(p, k):
                params[k] = p_list[p]  # keep it
    return params
