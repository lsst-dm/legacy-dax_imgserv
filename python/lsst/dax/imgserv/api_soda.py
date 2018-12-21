# LSST Data Management System
# Copyright 2017-2019 AURA/LSST.
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
This module implements the REST interface for Image Cutout Service.
Corresponding URI: /image

"""
import os
import traceback
import tempfile
import json

from flask import Blueprint, make_response, request, current_app
from flask import render_template, send_file, jsonify

import lsst.log as log
import lsst.afw.image as afwImage

from .vo.imageSODA import ImageSODA


image_api_soda = Blueprint("api_image_soda", __name__, static_folder="static",
                           template_folder="templates")


def load_imgserv_config(config_path, metaserv_url):
    """ Load configuration info into ImageServ.
        To be called from webserv.

    """
    if config_path is None:
        # use default root_path for image_api_soda
        config_path = image_api_soda.root_path+"/config/"
    f_json = os.path.join(config_path, "imgserv_conf.json")
    # load the general config file
    current_app.config.from_json(f_json)
    # configure the log file (log4cxx)
    log.configure(os.path.join(config_path, "log.properties"))
    current_app.config["DAX_IMG_META_URL"] = metaserv_url
    current_app.config["DAX_IMG_CONFIG"] = config_path
    current_app.config["imageREST_soda"]=os.path.join(config_path,
                                                      "imageREST_v1.schema")
    # create cache for butler instances
    current_app.butler_instances = {}
    # create SODA service
    f_dashboard = os.path.join(config_path, "dashboard.json")
    with open(f_dashboard, "r") as f:
        dashboard = json.load(f)
    current_app.soda = ImageSODA(current_app.config, dashboard)


@image_api_soda.route("/")
def index():
    return make_response(render_template("api_image_soda.html"))


@image_api_soda.route("/availability", methods=["GET"])
def imgsoda_availability():
    xml = current_app.soda.get_availability(_getparams())
    return make_response(xml)


@image_api_soda.route("/capabilities", methods=["GET"])
def imgsoda_capabilities():
    xml = current_app.soda.get_capabilities(_getparams())
    return make_response(xml)


@image_api_soda.route("/examples", methods=["GET"])
def imgsoda_examples():
    html = current_app.soda.get_examples(_getparams())
    return make_response(html)


@image_api_soda.route("/sync", methods=["GET", "PUT", "POST"])
def imgsoda_sync():
    params = _getparams()
    image = current_app.soda.do_sync(params)
    if image:
        return _data_response(image)
    else:
        return _image_not_found("Image Not Found")


@image_api_soda.route("/async", methods=["POST"])
def imgsoda_async():
    params = _getparams()
    xml = current_app.soda.do_async(params)
    return make_response(xml)


@image_api_soda.errorhandler(Exception)
def imgsoda_unhandled_exceptions(error):
    err = {
        "exception": error.__class__.__name__,
        "message": error.args[0],
        "traceback": traceback.format_exc()
    }
    if len(error.args) > 1:
        err["more"] = [str(arg) for arg in error.args[1:]]
    response = err
    return response


def _getparams():
    if request.content_type and "form" in request.content_type:
        # e.g. Content-Type: application/www-form-urlencoded
        params = request.form.copy()
    else:
        # GET
        params = request.args.copy()
    # Mark the type for later reference
    params["API"] = "SODA"
    return params

def _data_response(image):
    """Write image data to FITS file and send back.

    Parameters
    ----------
    image: lsst.afw.image.Exposure

    Returns
    -------
    flask.send_file
        the image as FITS file attachement.
    """
    if isinstance(image, (afwImage.Exposure, afwImage.Image)):
        fp = tempfile.NamedTemporaryFile()
        image.writeFits(fp.name)
        res = send_file(fp.name,
                        mimetype="image/fits",
                        as_attachment=True,
                        attachment_filename="image.fits")
    else:
        res = jsonify(image)
    return res


def _image_not_found(message=None):
    # HTTP 404 - NOT FOUND, RFC2616, Section 10.4.5
    if not message:
        message = "Image Not Found"
    response = jsonify({"exception": IOError.__name__, "message": message})
    response.status_code = HTTPStatus.NOT_FOUND  # ValueError == BAD REQUEST
    return response
