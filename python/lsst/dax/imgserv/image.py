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
This module implements the Image request data model and operations.

@author: Kenny Lo, SLAC

"""

class Image(object):
    """ Image module maps a request and its parameters per JSON schema to the 
        corresponding imagegetter method. 
    """
  
    def __init__(self):
       pass

    @classmethod
    def full(cls, img_getter, params):
        """ params(dict): ra, dec filt """
        return img_getter.fullimage(cls._kp_val("ra", params), 
                cls._kp_val("dec", params), 
                cls._kp_val("filter", params))
        
    @classmethod
    def by_data_id(cls, img_getter, params):
        """ params(dict): data_id """
        ids = cls._kp_dict("data_id", params)
        # for data_id_tpf,fix the patch field
        if "patch_x" in ids:
            ids["patch"] = "%d,%d" % (ids["patch_x"], ids["patch_y"])
            del ids["patch_x"]
            del ids["patch_y"]
        return img_getter.image_by_data_id(ids)
    
    @classmethod
    def cutout_by_data_id(cls, img_getter, params):
        """ params(dict): data_id """
        ids = cls._kp_dict("data_id", params)
        # for data_id_tpf,fix the patch field
        if "patch_x" in ids:
            ids["patch"] = "%d,%d" % (ids["patch_x"], ids["patch_y"])
            del ids["patch_x"]
            del ids["patch_y"]
        return img_getter.imagecutout_from_data_id(ids, 
                cls._kp_val("ra", params),
                cls._kp_val("dec", params),
                cls._kp_val("width", params),
                cls._kp_val("height", params),
                cls._kp_val("unit", params))

    @classmethod
    def by_science_id(cls, img_getter, params):
        """ params(dict): science_id, science_id_type) """
        return img_getter.image_from_science_id(cls._kp_val("science_id",
            params))

    @classmethod
    def cutout(cls, img_getter, params):
        """ params(dict): ra, dec, filt, width, height, unit """
        return img_getter.image_cutout(cls._kp_val("ra", params),
                cls._kp_val("dec", params),
                cls._kp_val("filter", params),
                cls._kp_val("width", params),
                cls._kp_val("height", params),
                cls._kp_val("unit", params))
    
    @classmethod
    def cutout_by_science_id(cls, img_getter, params):
        """ params (dict): science_id, ra, dec, width, height, unit """
        return img_getter.imagecutout_from_science_id(
                cls._kp_val("science_id", params),
                cls._kp_val("ra", params),
                cls._kp_val("dec", params),
                cls._kp_val("width", params),
                cls._kp_val("height", params),
                cls._kp_val("unit", params))

    @classmethod
    def _kp_val(cls, key, params):
        for p in params.keys():
            if key in p:
                elems = p.split(".")
                if key in elems[-1]:
                    return params[p]

    @classmethod
    def _kp_dict(cls, key, params):
        # return dict of parameter values
        p_dict = {}
        for p in params.keys():
            if key in p:
                elems = p.split(".")
                p_dict[elems[-1]] = params[p]
        return p_dict

