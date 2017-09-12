#!/usr/bin/env python
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

'''
This module implements the command line interface (CLI) to ImageServ.

@author: Brian Van Klaveren, SLAC
@author: Kenny Lo, SLAC

'''

import os
import json

import click

import lsst.log as log
import lsst.daf.persistence as dafPersist

from .locateImage import (
        image_open, W13DeepCoaddDb, W13RawDb, W13CalexpDb
)

from .image import Image
from .dispatch import Dispatcher
from .hashutil import Hasher

ROOT = os.path.abspath(os.path.dirname(__file__))


@click.command()
@click.argument("--config")
@click.argument("--in-req", type=click.Path(exists=True))
@click.argument("--out-dir", type=click.Path(exists=True))
def exec_command(config_dir, in_req, out_dir):
    """ Command Line Interface: Process query to return image file
        in output directory.
    """
    cli = ImageServCLI(config_dir, in_req, out_dir)
    cli.process_request()


class ImageServCLI(object):
    """ Module to implement CLI for ImageServ.

    """
    def __init__(self, config_dir, in_req, out_dir):
        # load the configuration file
        if config_dir:
            config = os.path.join(config_dir, "settings.json")
        else:
            config_dir = os.path.join(ROOT, "config")
            config = os.path.join(config_dir, "settings.json")
        with open(config) as f:
            self._config = json.load(f)
            # strip /tests from dataroot if cur_dir is /tests
            f.close()
         # configure the log file (log4cxx)
        log.configure(os.path.join(config_dir, "log.properties"))
        self._in_req = in_req
        self._out_dir = out_dir
        self._dispatcher = Dispatcher(config_dir)

    def process_request(self):
        errors, req = self._parse_req()
        w13db = self._get_ds(req)
        if w13db is not None:
            img_getter = image_open(w13db, self._config)
            result = self.dispatch(img_getter, req)
            name = req["name"]
            fp_image = self._save_result(result, name)
            if self._check_result(req, fp_image):
                print(name+": PASS")
            else:
                print(name+": FAIL")
        else:
            raise Exception("ImageServCLI", "w13db should NOT be None")

    def dispatch(self, img_getter, req):
        # direct the request to the best fit image API on parameters
        api, params = self._dispatcher.find_api(req)
        # call the API with the params
        try:
            result = api(img_getter, params)
            return result
        except:
            raise Exception("dispatch failed")

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
        check_code = req["check"]["sha256"]
        with open(image_fp, "rb") as f:
            image_data = f.read()
            f.close()
        h = Hasher.hash(image_data).hexdigest()
        return True if h == check_code else False

    def _get_ds(self, req):
        # determine and get the image datasource
        image_type = req["get"]["api_id"][0]
        if image_type == "raw":
            return W13RawDb
        elif image_type == "calexp":
            return W13CalexpDb
        elif image_type == "deepcoadd":
            return W13DeepCoaddDb

    def _get_req_data(self):
        with open(self._in_req, "r") as req_file:
            data = req_file.read()
            req_file.close()
        return data

    def _parse_req(self):
        req_data = self._get_req_data()
        # read and parse the request (JSON)
        req = json.loads(req_data)
        # ToDo: validate schema here (DM-9929)
        errors = 0
        return errors, req


if __name__ == '__main__':
    exec_command()



