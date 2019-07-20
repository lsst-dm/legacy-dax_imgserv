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
import base64
from http import HTTPStatus

from flask import Blueprint, make_response, request, current_app
from flask import render_template, send_file, jsonify, url_for, redirect
from werkzeug.exceptions import HTTPException

from jsonschema import validate

import lsst.log as log

from .vo.imageSODA import ImageSODA
from .jsonutil import get_params
import redis

image_soda = Blueprint("api_image_soda", __name__, static_folder="static",
                       template_folder="templates")


# log the user name of the auth token
@image_soda.before_request
def check_auth():
    """ Data Formats
    HTTP Header
        Authorization: Bearer <JWT token>
    JWT token
        header.payload.signature[optional])
    """
    auth_header = request.headers.get("Authorization")
    if auth_header:
        try:
            auth_header_parts = auth_header.split(" ")
            atk = auth_header_parts[1]
            p = atk.split(".")[1]
            p = p + ('=' * (len(p) % 4)) # padding for b64
            p = base64.urlsafe_b64decode(p)
            user_name = json.loads(p).get("uid")
            log.info("JWT received for user: {}".format(user_name))
        except(UnicodeDecodeError, TypeError, ValueError):
            log.info("unexpected error in JWT")


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
    if metaserv_url:
        current_app.config["DAX_IMG_META_URL"] = metaserv_url
    else:
        current_app.config["DAX_IMG_META_URL"] = current_app.config[
            "dax.imgserv.meta.url"]
    current_app.config["imgserv_api"] = os.path.join(config_path,
                                                     "image_api_schema.json")
    # create cache for butler instances
    current_app.butler_instances = {}
    # create SODA service
    current_app.soda = ImageSODA(current_app.config)


@image_soda.route("/")
def img_index():
    return make_response(render_template("api_image_soda.html"))


@image_soda.route("/availability", methods=["GET"])
def img_availability():
    xml = current_app.soda.get_availability(_getparams())
    resp = make_response(xml)
    resp.headers["Content-Type"] = "text/xml"
    return resp


@image_soda.route("/capabilities", methods=["GET"])
def img_capabilities():
    xml = current_app.soda.get_capabilities(_getparams())
    resp = make_response(xml)
    resp.headers["Content-Type"] = "text/xml"
    return resp


@image_soda.route("/examples", methods=["GET"])
def img_examples():
    html = current_app.soda.get_examples(_getparams())
    return make_response(html)


@image_soda.route("/tables", methods=["GET"])
def img_tables():
    xml = current_app.soda.get_tables(_getparams())
    return make_response(xml)


@image_soda.route("/sia", methods=["GET"])
def img_sia():
    resp = current_app.soda.do_sia(_getparams())
    return make_response(resp)


@image_soda.route("/sync", methods=["GET", "PUT", "POST"])
def img_sync():
    if not request.args:
        soda_url = url_for('api_image_soda.img_sync', _external=True)
        # no parameters present, return service status
        return _service_response(soda_url)
    image = current_app.soda.do_sync(_getparams())
    if image:
        return _data_response(image)
    else:
        return _image_not_found()


@image_soda.route("/async", methods=["GET", "POST"])
def img_async():
    if not request.args:
        # no parameters present, return service status
        soda_url = url_for('api_image_soda.img_async', _external=True)
        return _service_response(soda_url)
    new_job = current_app.soda.do_async(_getparams())
    return redirect(url_for('api_image_soda.img_async_jobs_id',
                            job_id = new_job.job_id,
                            _external=True))


@image_soda.route("/async-jobs", methods=["GET"])
def img_async_jobs():
    # return all jobs in the system
    return


@image_soda.route("/async-jobs/<job_id>", methods=["GET"])
def img_async_jobs_id(job_id):
    # return the job info
    r = redis.Redis()
    result = r.get(job_id)
    resp = ""
    if result:

        return resp


@image_soda.route("/async-jobs/<job_id>/parameters", methods=["GET", "POST"])
def img_async_jobs_parameters(job_id):
    rd = redis.Redis()
    qr = rd.get(job_id)
    if request.method == "POST":
        return

    if qr.phase == "PENDING" and request.method == "POST":
        # PENDING: can update the parameters
        qr.params = _getparams()
    return

@image_soda.route("/async-jobs/<job_id>/results", methods=["GET"])
def img_async_jobs_results(job_id):
    # return the list of result URI(s)
    return


@image_soda.route("/async-jobs/<job_id>/results/<result_id>", methods=["GET"])
def img_async_jobs_result(job_id, result_id):
    # retrieve the image result
    rd = redis.Redis()
    res = rd.get(job_id)
    if result_id in res.results:
        image_file =res.results["result_id"]
    if res.phase == "COMPLETED":
        resp = send_file(image_file,
                    mimetype="image/fits",
                    as_attachment=True,
                    attachment_filename="image.fits")
        return resp
    else:
        return _uws_job_response(res.job_url)


@image_soda.route("/async-jobs/<job_id>/error", methods=["GET"])
def img_async_jobs_error(job_id):
    # return the error info
    return


@image_soda.errorhandler(HTTPException)
@image_soda.errorhandler(Exception)
def unhandled_exceptions(error):
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


def _service_response(soda_url):
    """ Return service info using DALI template.
    """
    resp = make_response(render_template("soda_descriptor.xml",
                                         soda_ep=soda_url),
                         HTTPStatus.OK)
    resp.headers["Content-Type"] = "text/xml"
    return resp


def _uws_job_response(uws_job_result_url):
    """ Return job info using UWS template.
    """
    resp = make_response(render_template("uws_job_result.xml",
                                         job_result_url=uws_job_result_url),
                         HTTPStatus.OK)
    resp.headers["Content-Type"] = "text/xml"
    return resp


@image_soda.errorhandler(HTTPStatus.NOT_FOUND)
def _image_not_found():
    """ Return a generic error using DALI template.
    """
    message = "Image Not Found"
    resp = make_response(render_template("dali_response.xml",
                                         dali_resp_state="Error",
                                         dali_resp_msg=message),
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
    # Mark the API variant for later reference
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

