#!/usr/bin/env python
#
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
This module implements the command line interface (CLI) to ImageServ.

"""

import os
import json
from jsonschema import validate, ValidationError

import click

import lsst.log as log

from lsst.dax.imgserv.hashutil import Hasher
from lsst.dax.imgserv.locateImage import open_image
from lsst.dax.imgserv.dispatch import Dispatcher
from lsst.dax.imgserv.jsonutil import flatten_json

import etc.imgserv.imgserv_config as imgserv_config

ROOT = os.path.abspath(os.path.dirname(__file__))

imgserv_meta_url = imgserv_config.webserv_config.get("dax.imgserv.meta.url", None)


@click.command()
@click.option("--ds")
@click.option("--query")
@click.option("--out_dir", type=click.Path(exists=True))
def exec_cli(ds, query, out_dir):
    """ Command Line Interface: Process query to return image file
        in output directory.

    Parameters
    ----------
    ds : `str`
        the dataset identifier.
    query : `str`
        the request query.
    out_dir : `str`
        the output directory
    """
    if query is None or out_dir is None:
        print("Missing input parameter(s)")
    else:
        cli = ImageServCLI(ds, out_dir)
        cli.process_request(query)


class ImageServCLI(object):
    """ Module to implement the command line interface for ImageServ.

    Parameters
    ----------
    ds : `str`
        the dataset identifier.
    out_dir : `str`
        the output directory.
    """
    def __init__(self, ds, out_dir):
        # load the configuration file
        config_dir = os.path.join(ROOT, "config")
        if ds is None or ds == "default":
            ds = imgserv_config.config_datasets["default"]
        self._config = imgserv_config.config_datasets[ds]
        # configure the log file (log4cxx)
        log.configure(os.path.join(config_dir, "log.properties"))
        self._out_dir = out_dir
        self._dispatcher = Dispatcher(config_dir)
        self._schema = os.path.join(config_dir, "image_api_schema.json")
        self._validate = self._config.get("DAX_IMG_VALIDATE", False)
        self._config["DAX_IMG_META_URL"] = imgserv_meta_url

    def process_request(self, in_req):
        """ Process the request.

        Parameters
        ----------
            in_req : `str`
            the file pathname containing the JSON query OR param1=value1&param2=value2&...
        """
        if os.path.isfile(in_req):
            req = self._parse_req_in_file(in_req)
        else:
            req = self._parse_req_str(in_req)
        ds, ds_type, filt = req["ID"].split(".")
        req["ds"] = ds
        req["filter"] = filt
        # make sure the corect ds configuration is used
        if ds == "default":
            ds = imgserv_config.config_datasets["default"]
            self._config = imgserv_config.config_datasets[ds]
        img_getter = open_image(ds, ds_type, self._config)
        result = self._dispatch(img_getter, req)
        f_name = ds_type + Hasher.md5(in_req)
        fn = self._save_result(result, f_name)
        if self._check_result(fn):
            print( f"Output = {fn}")
        else:
            print(f"{f_name} is invalid FITS")

    def _dispatch(self, img_getter, req):
        # direct the request to the best fit image API on parameters
        api, api_params = self._dispatcher.find_api(req)
        # call the API with the params
        result = api(img_getter, api_params)
        return result

    def _save_result(self, image, fn_prefix="image_out"):
        # save image object into fits file in output directory
        fn = os.path.join(self._out_dir, fn_prefix+".fits")
        if image is None:
            with open(fn, "w") as f:
                f.write("Image Not Found")
                f.close()
        else:
            image.writeFits(fn)
        return fn

    @staticmethod
    def _check_result(image_fp):
        with open(image_fp, "rb") as f:
            first_line = str(f.read(80))
            if "does conform to FITS standard" in first_line:
                return True
            else:
                return False

    def _get_params(self, req):
        """ Get the parameters corresponding to the API.
        The extraction of each parameter is based upon best match
        of the name with parts delimited by '.'.

        Parameters
        ----------
        req: `dict`
            the request in JSON format

        Returns
        -------
        params : `dict`
            the list of parameters and their values.
        """
        image = req.get("image", None)
        p_list = flatten_json(image)
        params = {}
        pos = ""
        for k in ["ID", "POS"]:
            for p in list(p_list):
                if self._endswith(p, k):
                    params[k] = p_list[p]
                elif "POS" in p:
                    pos += f" {p_list[p]}"
                p_list.pop(p)
        if pos != "":
            # get the shape
            shape = list(image["cutout"]["POS"])[0]
            params["POS"] = shape.upper() + pos
        return params

    @staticmethod
    def _endswith(param, k):
        if param.endswith("."+k):
            return True
        elif k == param:
            return True
        else:
            return False

    @staticmethod
    def _parse_req_str(in_req):
        if "&" in in_req:
            l_params = in_req.split("&")
        else:
            l_params = in_req.split()  # whitespace delimiter, as default
        req = {}
        for param in l_params:
            pv = param.split("=")
            req[pv[0]] = pv[1]
        if "ID" in in_req:
            if "POS" in in_req:
                req["POS"] = req["POS"].replace("+", " ")
                req["api_id"] = "POS"
            else:
                req["api_id"] = "ID"
        else:
            raise Exception("Invalid query")
        return req

    def _parse_req_in_file(self, in_req):
        with open(in_req) as f:
            req = json.load(f)
        try:
            if self._validate:
                # validate the schema
                with open(self._schema) as s:
                    schema = json.load(s)
                    s.close()
                validate(req, schema)
        except ValidationError as e:
            raise Exception(f"Validation Error {e.message}")
        params = self._get_params(req)
        return params


if __name__ == '__main__':
    exec_cli()

