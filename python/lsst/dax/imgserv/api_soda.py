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
This module implements the REST interface for Image Cutout Service.
Corresponding URI: /api/image/soda

"""
import os
import traceback
import tempfile
import json
from http import HTTPStatus

from flask import Blueprint, make_response, request, current_app
from flask import render_template, send_file, jsonify
from werkzeug.exceptions import HTTPException

from jsonschema import validate

import lsst.log as log

from .vo.imageSODA import ImageSODA
from .jsonutil import get_params

image_soda = Blueprint("api_image_soda", __name__, static_folder="static",
                           template_folder="templates")


def load_imgserv_config(config_path=None, metaserv_url=None):
    """ Load service configuration into ImageServ.

    Parameters
    ----------
    config_path : `str`
        configuration location of this service.
    metaserv_url : `str`
        service url of the metaserv instance.
    """
    if config_path is None:
        # use default root_path for app
        config_path = image_soda.root_path+"/config/"
    f_json = os.path.join(config_path, "imgserv_conf.json")
    # load the general config file
    current_app.config.from_json(f_json)
    # configure the log file (log4cxx)
    log.configure(os.path.join(config_path, "log.properties"))
    current_app.config["DAX_IMG_CONFIG"] = config_path
    if metaserv_url is not None:
        current_app.config["dax.imgserv.meta.url"] = metaserv_url
    current_app.config["imgserv_api"]=os.path.join(config_path,
                                                   "image_api_schema.json")
    # create cache for butler instances
    current_app.butler_instances = {}
    # create SODA service
    f_dashboard = os.path.join(config_path, "dashboard.json")
    with open(f_dashboard, "r") as f:
        dashboard = json.load(f)
    current_app.soda = ImageSODA(current_app.config, dashboard)


@image_soda.route("/")
def index():
    return make_response(render_template("api_image_soda.html"))


@image_soda.route("/availability", methods=["GET"])
def imagesoda_availability():
    xml = current_app.soda.get_availability(_getparams())
    resp = make_response(xml)
    resp.headers["Content_Type"] = "text/xml"
    return resp


@image_soda.route("/capabilities", methods=["GET"])
def imagesoda_capabilities():
    xml = current_app.soda.get_capabilities(_getparams())
    resp = make_response(xml)
    resp.headers["Content_Type"] = "text/xml"
    return resp


@image_soda.route("/examples", methods=["GET"])
def imagesoda_examples():
    html = current_app.soda.get_examples(_getparams())
    return make_response(html)


@image_soda.route("/sync", methods=["GET", "PUT", "POST"])
def imagesoda_sync():
    image = current_app.soda.do_sync(_getparams())
    if image:
        return _data_response(image)
    else:
        return _image_not_found()


@image_soda.route("/async", methods=["POST"])
def imagesoda_async():
    params = _getparams()
    xml = current_app.soda.do_async(params)
    resp = make_response(xml)
    resp.headers["Content_Type"] = "text/xml"
    return resp


@image_soda.errorhandler(HTTPException)
@image_soda.errorhandler(Exception)
def imagesoda_unhandled_exceptions(error):
    err = {
        "exception": error.__class__.__name__,
        "message": error.args[0],
        "traceback": traceback.format_exc()
    }
    if len(error.args) > 1:
        err["more"] = [str(arg) for arg in error.args[1:]]
    resp = jsonify(err)
    resp.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
    return resp


@image_soda.errorhandler(HTTPStatus.NOT_FOUND)
def _image_not_found():
    """ Return a generic error using DALI template.
    """
    message = "Image Not Found"
    resp = make_response(render_template("dali_error.xml",
                                         dali_error_msg=message),
                         HTTPStatus.NOT_FOUND)
    resp.headers["Content-Type"] = "text/xml"
    return resp


def _getparams():
    if request.is_json:
        r_data = request.get_json()
        # schema validation check
        check = current_app.config["DAX_IMG_VALIDATE"]
        if check:
            f_schema = current_app.config["imgserv_api"]
            with open(f_schema) as f:
                schema = json.load(f)
            validate(r_data, schema)
        params = get_params(r_data)
    else:
        if request.content_type and "form" in request.content_type:
            # e.g. Content-Type: application/www-form-urlencoded
            params = request.form.copy()
        else:
            # GET
            params = request.args.copy()
    # Mark the API version for later reference
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
        the image as FITS file attachment.
    """
    fp = tempfile.NamedTemporaryFile()
    image.writeFits(fp.name)
    resp = send_file(fp.name,
                    mimetype="image/fits",
                    as_attachment=True,
                    attachment_filename="image.fits")
    return resp

