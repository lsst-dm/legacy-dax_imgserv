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
This module implements various JSON-related utility functions and classes.

@author: Kenny Lo, SLAC

"""
import json

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



