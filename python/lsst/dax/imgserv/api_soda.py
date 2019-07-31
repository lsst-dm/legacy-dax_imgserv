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

from flask import Blueprint, make_response, request, current_app, session
from flask import render_template, send_file, jsonify, url_for, redirect
from werkzeug.exceptions import HTTPException

from jsonschema import validate

import lsst.log as log

from .vo.imageSODA import ImageSODA
from .jsonutil import get_params
from .jobqueue.imageworker import make_celery, app_celery

image_soda = Blueprint("api_image_soda", __name__, static_folder="static",
                       template_folder="templates")

# map state (celery) to phase(IVOA UWS)
map_phase_from_state = {"SUCCESS": "COMPLETED",
                        "PENDING": "PENDING",
                        "QUEUED": "QUEUED",
                        "STARTED": "EXECUTING",
                        "FAILURE": "ERROR",
                        "REVOKED": "ABORTED",
                        "RETRY": "UNKNOWN",
                        "HELD": "HELD",            # ToDo: define HELD
                        "SUSPENDED": "SUSPENDED",  # ToDo: define SUSPENDED
                        "ARCHIVED": "ARCHIVED"     # ToDo: define ARCHIVED
                        }

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
            session["user"] = user_name # keep it in flask session object
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
    # Instantiate celery for client access
    current_app.celery = make_celery()


@image_soda.route("/")
def img_index():
    """ Get the service endpoint status. """
    return make_response(render_template("api_image_soda.html"))


@image_soda.route("/availability", methods=["GET"])
def img_availability():
    """ Get the service availability status. """
    xml = current_app.soda.get_availability(_getparams())
    resp = make_response(xml)
    resp.headers["Content-Type"] = "text/xml"
    return resp


@image_soda.route("/capabilities", methods=["GET"])
def img_capabilities():
    """ Get the service capabilities."""
    xml = current_app.soda.get_capabilities(_getparams())
    resp = make_response(xml)
    resp.headers["Content-Type"] = "text/xml"
    return resp


@image_soda.route("/examples", methods=["GET"])
def img_examples():
    """ Get /examples for the service. """
    html = current_app.soda.get_examples(_getparams())
    return make_response(html)


@image_soda.route("/tables", methods=["GET"])
def img_tables():
    """ Get /tables for the service. """
    xml = current_app.soda.get_tables(_getparams())
    return make_response(xml)


@image_soda.route("/sia", methods=["GET"])
def img_sia():
    """" Get the /sia service endpoint."""
    resp = current_app.soda.do_sia(_getparams())
    return make_response(resp)


@image_soda.route("/sync", methods=["GET", "PUT", "POST"])
def img_sync():
    """ Service the /sync request.

    Returns
    -------
    resp : `flask.Response`
        the response.
    """
    if not request.args:
        soda_url = url_for('api_image_soda.img_sync', _external=True)
        # no parameters present, return service status
        return _service_response(soda_url)
    image = current_app.soda.do_sync(_getparams())
    if image:
        with tempfile.NamedTemporaryFile(prefix="img_", suffix=".fits") as fp:
            image.writeFits(fp.name)
            resp = send_file(fp.name,
                             mimetype="image/fits",
                             as_attachment=True,
                             attachment_filename=os.path.basename(fp.name))
            return resp
    else:
        return _image_not_found()


@image_soda.route("/async", methods=["GET", "POST"])
def img_async():
    """ Get the /async service endpoint. """
    if not request.args:
        # no parameters present, return service status
        soda_url = url_for('api_image_soda.img_async', _external=True)
        return _service_response(soda_url)
    job_id = current_app.soda.do_async(_getparams())
    return redirect(url_for('api_image_soda.img_async_jobs_job',
                            job_id=job_id,
                            _external=True))


@image_soda.route("/async-jobs", methods=["GET"])
def img_async_jobs():
    """ Get all the jobs of the user in the system. """
    if session.get("user"):
        xml = current_app.soda.get_jobs(_getparams())
        resp = make_response(xml)
        resp.headers["Content-Type"] = "text/xml"
        return resp
    else:
        raise Exception("Error: user not specified")


@image_soda.route("/async-jobs/<job_id>", methods=["GET"])
def img_async_jobs_job(job_id: str):
    """ Get the job info, else path to the result if ready.

    Parameters
    ----------
    job_id: `str`

    Returns
    -------
    xml: `str`
        the job description.
    """
    ar = app_celery.AsyncResult(job_id)
    if ar.state == "SUCCESS":
        return redirect(url_for('api_image_soda.img_async_job_results',
                                job_id=job_id,
                                _external=True))

    else:
        job_result_url = url_for('api_image_soda.img_async_job_results',
                                 job_id = job_id,
                                 _external=True)
        return _uws_job_response(job_result_url)


@image_soda.route("/async-jobs/<job_id>/phase", methods=["GET"])
def img_async_job_phase(job_id: str):
    """ Get the phase info for the job.

    Parameters
    ----------
    job_id : `str`

    Returns
    -------
    xml: `str`
        phase for the job.
    """
    ar = app_celery.AsyncResult(job_id)
    phase = map_phase_from_state[ar.state]
    return _uws_job_response_plain("PHASE="+phase)


@image_soda.route("/async-jobs/<job_id>/executionduration", methods=["GET"])
def img_async_job_duration(job_id: str):
    """ Get the duration in number of seconds for the job.

    Parameters
    ----------
    job_id : `str`

    Returns
    -------
    xml: `str`
        the job duration info.
    """
    return _uws_job_response_plain("INFO=NOT IMPLEMENTED")


@image_soda.route("/async-jobs/<job_id>/destruction", methods=["GET"])
def img_async_job_destruction(job_id: str):
    """ Get the destruction instant for the job.

    Parameters
    ----------
    job_id : `str`

    Returns
    -------
    xml : `str`
        the destruction instant for the job.
    """
    return _uws_job_response_plain("INFO=NOT IMPLEMENTED")


@image_soda.route("/async-jobs/<job_id>/error", methods=["GET"])
def img_async_job_error(job_id: str):
    """ Get the error info for the job.

    Parameters
    ----------
    job_id : `str`
    Returns
    -------
    xml : `str`
        the error info.
    """
    ar = app_celery.AsyncResult(job_id)
    phase = map_phase_from_state[ar.state]
    e = ar.get() if phase == "ERROR" else "NONE"
    return _uws_job_response_plain("ERROR="+e)


@image_soda.route("/async-jobs/<job_id>/quote", methods=["GET"])
def img_async_job_quote(job_id: str):
    """ Get the quote for the job.

    Parameters
    ----------
    job_id : `str`
    Returns
    -------
    xml : `str`
        the quote info for the job.
    """
    return _uws_job_response_plain("INFO=NOT IMPLEMENTED")


@image_soda.route("/async-jobs/<job_id>/results", methods=["GET"])
def img_async_job_results(job_id: str):
    """ Get the results for the job.

    Parameters
    ----------
    job_id : `str`

    Returns
    -------
    xml : `str`
        the job results.
    """
    # For now redirect to single result
    return redirect(url_for('api_image_soda.img_async_job_results_result',
                            job_id=job_id,
                            _external=True))


@image_soda.route("/async-jobs/<job_id>/results/result", methods=["GET"])
def img_async_job_results_result(job_id: str):
    """ Get the single result for the job.

    Parameters
    ----------
    job_id : `str`

    Returns
    -------
    xml : `str`
        the job results.
    """
    ar = app_celery.AsyncResult(job_id)
    if ar.state == "SUCCESS":
        result = ar.get()  # retrieve path to the image output
        resp = send_file(result,
                         mimetype="image/fits",
                         as_attachment=True,
                         attachment_filename=os.path.basename(result))
        return resp
    else:
        return redirect(url_for('api_image_soda.img_async_jobs_job',
                                job_id=job_id,
                                _external=True))


@image_soda.route("/async-jobs/<job_id>/parameters", methods=["GET", "POST"])
def img_async_job_parameters(job_id: str):
    """Get/Set the parameters for the job.

    Parameters
    ----------
    job_id : `str`
    Returns
    -------
     xml : `str`
        the job parameters.
    """
    ar = app_celery.AsyncResult(job_id)
    if ar.state in ["PENDING", "STARTED"]:
        params=str(ar.args)
    else:
        params = "NOT AVAILABLE"
    if ar.state == "PENDING" and request.method == "POST":
        # TODO: update the job parameters in PENDING state
        new_params = _getparams()
    return _uws_job_response_plain("PARAMS="+params)


@image_soda.route("/async-jobs/<job_id>/owner", methods=["GET"])
def img_async_job_owner(job_id: str):
    """ Get the owner info for the job.

    Parameters
    ----------
    job_id : `str`

    Returns
    -------
    xml : `str`
        the job owner.
    """
    ar = app_celery.AsyncResult(job_id)
    # ideally owner should be stored somewhere along with the job_id
    # and assert session against that.
    user = session["user"] if session.get("user") else "UNKNOWN"
    return _uws_job_response_plain("OWNER="+user)


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
    """ Get the service info using DALI template.
    Parameters
    ----------
    soda_url : `str`
    """
    resp = make_response(render_template("soda_descriptor.xml",
                                         soda_ep=soda_url),
                         HTTPStatus.OK)
    resp.headers["Content-Type"] = "text/xml"
    return resp


def _uws_job_response(uws_job_result_url: str):
    """ Get the job info using UWS template.

    Parameters
    ----------
    uws_job_result_url : `str'

    Returns
    -------
    resp : `str`
        the response in XML.
    """
    resp = make_response(render_template("uws_job_result.xml",
                                         job_result_url=uws_job_result_url),
                         HTTPStatus.OK)
    resp.headers["Content-Type"] = "text/xml"
    return resp


def _uws_job_response_plain(info: str):
    """ Generate the generic textual response.
    Parameters
    ----------
    info : `str`
        the info string.
    """
    resp = make_response(info,
                         HTTPStatus.OK)
    resp.headers["Content-Type"] = "text/plain"
    return resp


@image_soda.errorhandler(HTTPStatus.NOT_FOUND)
def _image_not_found():
    # Return generic error using DALI template.
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
