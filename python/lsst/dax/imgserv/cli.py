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

@author: Brian Van Klaveren, SLAC
@author: Kenny Lo, SLAC

"""

import os
import json
from jsonschema import validate, ValidationError

import click

import lsst.log as log

from configparser import RawConfigParser

from lsst.dax.imgserv.locateImage import image_open
from lsst.dax.imgserv.dispatch import Dispatcher
from lsst.dax.imgserv.hashutil import Hasher
from lsst.dax.imgserv.jsonutil import flatten_json
from lsst.dax.imgserv.locateImage import get_ds

ROOT = os.path.abspath(os.path.dirname(__file__))

defaults_file = os.environ.get("WEBSERV_CONFIG", "~/.lsst/webserv.ini")

# Initialize configuration
webserv_parser = RawConfigParser()
webserv_parser.optionxform = str

with open(os.path.expanduser(defaults_file)) as cfg:
    webserv_parser.readfp(cfg, defaults_file)

webserv_config = dict(webserv_parser.items("webserv"))
imgserv_meta_url = webserv_config.get("dax.imgserv.meta.url")


@click.command()
@click.option("--config", type=click.Path(exists=True))
@click.option("--query")
@click.option("--out", type=click.Path(exists=True))
def exec_command(config, query, out):
    """ Command Line Interface: Process query to return image file
        in output directory.
    """
    cli = ImageServCLI(config, out)
    cli.process_request(query)


class ImageServCLI(object):
    """ Module to implement CLI for ImageServ.
    """
    def __init__(self, config_dir, out_dir):
        # load the configuration file
        if config_dir:
            config = os.path.join(config_dir, "imgserv_conf.json")
        else:
            config_dir = os.path.join(ROOT, "config")
            config = os.path.join(config_dir, "imgserv_conf.json")
        with open(config) as f:
            self._config = json.load(f)
        # configure the log file (log4cxx)
        log.configure(os.path.join(config_dir, "log.properties"))
        self._out_dir = out_dir
        self._dispatcher = Dispatcher(config_dir)
        self._schema = os.path.join(config_dir, "image_api_schema.json")
        self._validate =  self._config["DAX_IMG_VALIDATE"]
        self._config["DAX_IMG_META_URL"] = imgserv_meta_url

    def process_request(self, in_req):
        """ Process the request.

        Parameters
        ----------
            in_req - the file pathname containing the request
        """
        errors, req = self._parse_req(in_req)
        if errors > 0:
            raise Exception("parse error in req")
        image_type = req["image"]["ds"]
        w13db = get_ds(image_type)
        self._config["DAX_IMG_META_DB"] = req["image"]["db"]
        img_getter = image_open(w13db, self._config)
        result = self._dispatch(img_getter, req)
        name = req["name"]
        fp_image = self._save_result(result, name)
        if self._check_result(req, fp_image):
            print(name+": PASSED")
        else:
            print(name+": FAILED")

    def _dispatch(self, img_getter, req):
        # direct the request to the best fit image API on parameters
        req_params = self._get_params(req)
        api, api_params = self._dispatcher.find_api(req_params)
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

    def _check_result(self, req, image_fp):
        check_code = req["check_output"]["md5"]
        with open(image_fp, "rb") as f:
            image_data = f.read()
        h = Hasher.md5(image_data)
        return True if h == check_code else False

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
        keys = req["api_id"]
        image = req["image"]
        p_list = flatten_json(image)
        # params to be list of values for the keys
        params = {}
        for k in keys:
            for p in p_list:
                if self._endswith(p, k):
                    params[k] = p_list[p]  # keep it
        params["API"] = req["API"]
        if req["API"] == "SODA":
            # get values of POS parameter
            pos_items = params["ID"].split(".")
            if len(pos_items) < 3:
                raise Exception("Invalid value for ID parameter")
            params["db"] = pos_items[0]
            params["ds"] = pos_items[1]
            params["filter"] = pos_items[2]
        return params

    def _endswith(self, param, k):
        if param.endswith("."+k):
            return True
        elif k == param:
            return True
        else:
            return False

    def _parse_req(self, in_req):
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
            msg = json.dumps({"Validation Error": e.message})
            return 1, msg
        return 0, req


if __name__ == '__main__':
    exec_command()

