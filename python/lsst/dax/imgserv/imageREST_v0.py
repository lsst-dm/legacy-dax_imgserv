#!/usr/bin/env python

# LSST Data Management System
# Copyright 2015 AURA/LSST.
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
This module implements the RESTful interface for Image Cutout Service.
Corresponding URI: /image

@author  Jacek Becla, SLAC; John Gates, SLAC; Kenny Lo, SLAC;
"""

import os
import tempfile
import traceback

from flask import Blueprint, make_response, request, current_app, jsonify
from flask import render_template

import lsst.afw.coord as afw_coord
import lsst.afw.geom as afw_geom
import lsst.log as log

from http.client import BAD_REQUEST, INTERNAL_SERVER_ERROR, NOT_FOUND
from .locateImage import (
        image_open, W13DeepCoaddDb, W13RawDb, W13CalexpDb
)
from .skymapStitch import getSkyMap


imageREST = Blueprint('imageREST', __name__, static_folder='static',
                      template_folder='templates')


# To be called from webserv
def load_imgserv_config(config_path, db_auth_conf):
    """Load configuration into ImageServ."""
    if config_path is None:
        # use default root_path for imageREST
        config_path = imageREST.root_path+"/config/"
    f_json = os.path.join(config_path, "settings.json")
    # load the general config file  
    current_app.config.from_json(f_json)
    # configure the log file (log4cxx)
    log.configure(os.path.join(config_path, "log.properties"))
    current_app.config['DAX_IMG_DBCONF'] = db_auth_conf


@imageREST.errorhandler(Exception)
def handle_unhandled_exceptions(error):
    err = {
        "exception": error.__class__.__name__,
        "message": error.args[0],
        "traceback": traceback.format_exc()
    }

    if len(error.args) > 1:
        err["more"] = [str(arg) for arg in error.args[1:]]
    response = jsonify(err)
    response.status_code = INTERNAL_SERVER_ERROR
    return response


# this will eventually print list of supported versions
@imageREST.route('/')
def index():
    return make_response(render_template("index.html"))

def _assert_ra_dec_filter(ra, dec, filter, valid_filters):
    """Validate ra, dec, filter. Return coerced values if valid,
    otherwise, raise a ValueError
    """
    if filter is None or filter not in valid_filters:
        msg = "Invalid filter {}. " \
              "Valid filters are {}.".format(filter, valid_filters)
        raise ValueError(msg)
    ra, dec = _assert_ra_dec(ra, dec)
    return ra, dec, filter

def _assert_ra_dec_filter(ra, dec, filter, valid_filters):
    """Validate ra, dec, filter. Return coerced values if valid,
    otherwise, raise a ValueError
    """
    if filter is None or filter not in valid_filters:
        msg = "Invalid filter {}. " \
              "Valid filters are {}.".format(filter, valid_filters)
        raise ValueError(msg)
    ra, dec = _assert_ra_dec(ra, dec)
    return ra, dec, filter

def _assert_ra_dec(ra, dec):
    """Validate ra, dec. Return coerced values if valid, otherwise,
    raise a ValueError
    """
    try:
        ra = float(ra)
        dec = float(dec)
    except ValueError:
        msg = "Invalid ra {} and dec {}. ".format(ra, dec)
        raise ValueError(msg)
    return ra, dec

def _assert_cutout_parameters(science_id, ra, dec, width, height, unit):
    """Validate science_id, ra, dec, filter, width, height, and unit.
    . Return coerced values if valid, otherwise, raise a ValueError
    """
    ra, dec = _assert_ra_dec(ra, dec)
    if unit == 'arcsec':
        width = float(width)
        height = float(height)
    elif unit == 'pixel':
        width = int(width)
        height = int(height)
    else:
        msg = "No Units specified for cutout dimensions"
        raise ValueError(msg)
    science_id = int(science_id)
    return science_id, ra, dec, width, height, unit


@imageREST.errorhandler(ValueError)
def handle_invalid_input(error):
    response = jsonify({"exception": "ValueError", "message": error.args[0]})
    response.status_code = 400  # ValueError == BAD REQUEST
    return response


# this will handle something like:
# GET /image/v0/raw?ra=359.195&dec=-0.1055&filter=r
@imageREST.route('/raw', methods=['GET'])
def getRaw():
    return _image(request, W13RawDb)


# this will handle something like:
# GET /image/v0/raw/ids?run=5646&camcol=4&field=694&filter=g
@imageREST.route('/raw/ids', methods=['GET'])
def getIRawIds():
    return _image_from_data_id(request, W13RawDb)


# this will handle something like:
# GET /image/v0/raw/id?id=3325410171
# Which should translate to run=3325 camcol=1 field=171 filter=z
@imageREST.route('/raw/id', methods=['GET'])
def getIRawScienceId():
    return _image_from_science_id(request, W13RawDb)


# this will handle something like:
# GET /image/v0/raw/cutout?ra=359.195&dec=-0.1055&filter=r&width=30.0&height=45.0
@imageREST.route('/raw/cutout', methods=['GET'])
def getIRawCutout():
    return _image_cutout(request, W13RawDb, 'arcsec')


# this will handle something like:
# GET /image/v0/raw/cutoutPixel?ra=359.195&dec=-0.1055&filter=r&width=30.0&height=45.0
@imageREST.route('/raw/cutoutPixel', methods=['GET'])
def getIRawCutoutPixel():
    return _image_cutout(request, W13RawDb, 'pixel')


# this will handle something like:
# GET /image/v0/raw/5646240694/cutout?ra=37.6292&dec=0.104625widthAng=30.0&heightAng=45.0
# GET /image/v0/raw/5646240694/cutout?ra=37.6292&dec=0.104625&widthPix=100&heightPix=100
@imageREST.route('/raw/<id>/cutout', methods=['GET'])
def getIRawCoutFromScienceId(id):
    return _image_cutout_from_science_id(request, W13RawDb, id)


# this will handle something like:
# GET /image/v0/calexp?filter=r&ra=37.644598&dec=0.104625
@imageREST.route('/calexp', methods=['GET'])
def getCalexp():
    return _image(request, W13CalexpDb)


# this will handle something like:
# GET /image/v0/calexp/ids?run=5646&camcol=4&field=694&filter=g
@imageREST.route('/calexp/ids', methods=['GET'])
def getICalexpIds():
    return _image_from_data_id(request, W13CalexpDb)


# this will handle something like:
# GET /image/v0/calexp/id?id=3325410171
# Which should translate to run=3325 camcol=1 field=171 filter=z
@imageREST.route('/calexp/id', methods=['GET'])
def getICalexpScienceId():
    return _image_from_science_id(request, W13CalexpDb)


# this will handle something like:
# GET /image/v0/calexp/cutout?ra=37.644598&dec=0.104625&filter=r&width=30.0&height=45.0
@imageREST.route('/calexp/cutout', methods=['GET'])
def getICalexpCutout():
    return _image_cutout(request, W13CalexpDb, 'arcsec')


# this will handle something like:
# GET /image/v0/calexp/cutoutPixel?ra=37.644598&dec=0.104625&filter=r&width=30.0&height=45.0
@imageREST.route('/calexp/cutoutPixel', methods=['GET'])
def getICalexpCutoutPixel():
    return _image_cutout(request, W13CalexpDb, 'pixel')


# this will handle something like:
# GET /image/v0/calexp/5646240694/cutout?ra=37.6292&dec=-0.0658&widthAng=30.0&heightAng=45.0
# GET /image/v0/calexp/5646240694/cutout?ra=37.6292&dec=-0.0658&widthPix=30&heightPix=45
@imageREST.route('/calexp/<id>/cutout', methods=['GET'])
def getICalexpCutoutFromScienceId(id):
    return _image_cutout_from_science_id(request, W13CalexpDb, id)


# this will handle something like:
# GET /image/v0/deepCoadd?ra=19.36995&dec=-0.3146&filter=r
@imageREST.route('/deepCoadd', methods=['GET'])
def getDeepCoadd():
    return _image(request, W13DeepCoaddDb)


# this will handle something like:
# GET /image/v0/deepCoadd/ids?tract=0&patch=225,1&filter='i'
@imageREST.route('/deepCoadd/ids', methods=['GET'])
def getIDeepCoaddsIds():
    return _image_from_data_id(request, W13DeepCoaddDb)


# this will handle something like:
# GET /image/v0/deepCoadd/id?id=23986176
# Which should translate to tract= patch=1 filter=
@imageREST.route('/deepCoadd/id', methods=['GET'])
def getIDeepCoaddScienceId():
    return _image_from_science_id(request, W13DeepCoaddDb)


# this will handle something like:
# GET /image/v0/deepCoadd/cutout?ra=19.36995&dec=-0.3146&filter=r&width=115&height=235
@imageREST.route('/deepCoadd/cutout', methods=['GET'])
def getIDeepCoaddCutout():
    return _image_cutout(request, W13DeepCoaddDb, 'arcsec')


# this will handle something like:
# GET /image/v0/deepCoadd/cutoutPixel?ra=19.36995&dec=-0.3146&filter=r&width=115&height=235
@imageREST.route('/deepCoadd/cutoutPixel', methods=['GET'])
def getIDeepCoaddCutoutPixel():
    return _image_cutout(request, W13DeepCoaddDb, 'pixel')


# this will handle something like:
# GET /image/v0/deepCoadd/23986176/cutout?ra=19.36995&dec=-0.3146widthAng=30.0&heightAng=45.0
# GET /image/v0/deepCoadd/23986176/cutout?ra=19.36995&dec=-0.3146xx&widthPix=100&heightPix=100
@imageREST.route('/deepCoadd/<id>/cutout', methods=['GET'])
def getIDeepCoaddCutoutFromScienceId(id):
    return _image_cutout_from_science_id(request, W13DeepCoaddDb, id)


def _image(_request, image_db_class):
    """ Get a full image from the input parameters.
    image_db_class should be the appropriate class (W13DeepCoadDb, W13RawDb, etc.)
    """
    ra = _request.args.get('ra')
    dec = _request.args.get('dec')
    filter = _request.args.get('filter')

    # check inputs
    try:
        ra, dec, filter = _assert_ra_dec_filter(ra, dec, filter, 'irg')
    except ValueError as e:
        return _error(ValueError.__name__, e.args[0], BAD_REQUEST)

    log.info("raw ra={} dec={} filter={}".format(ra, dec, filter))
    # fetch the image here
    img_getter = image_open(image_db_class, current_app.config)
    full_img = img_getter.fullimage(ra, dec, filter)
    if full_img is None:
        return _image_not_found()
    log.debug("Full w=%d h=%d", full_img.getWidth(), full_img.getHeight())
    return _file_response(full_img, "full_image.fits")


def _image_from_data_id(_request, image_db_class):
    """ Get a full image from the field ids given.
    image_db_class should be the appropriate class (W13DeepCoadDb, W13RawDb, etc.)
    """
    # fetch the image here
    img_getter = image_open(image_db_class, current_app.config)
    ids, valid_ids = img_getter.data_id_from_request(_request)
    log.info("valid={} id {}".format(valid_ids, ids))
    if not valid_ids:
        resp = "INVALID_INPUT {}".format(ids)
        return resp
    full_img = img_getter.image_by_data_id(ids)
    if full_img is None:
        return _image_not_found()
    log.debug("Full w=%d h=%d", full_img.getWidth(), full_img.getHeight())
    return _file_response(full_img, "full_image.fits")


def _image_from_science_id(_request, image_db_class):
    """ Get a full image response from the id given.
    image_db_class should be the appropriate class (W13DeepCoadDb, W13RawDb, etc.)
    """
    img_getter = image_open(image_db_class, current_app.config)
    value = _request.args.get("id")
    if value is None:
        resp = "INVALID_INPUT value={}".format(value)
        return resp
    ids, valid = img_getter.data_id_from_science_id(value)
    log.debug("_getIScienceId valid={} value={} ids={}".format(valid, value, ids))
    if not valid:
        msg = "INVALID_INPUT value={} {}".format(value, ids)
        return _error(ValueError.__name__, msg, BAD_REQUEST)
    full_img = img_getter.image_by_data_id(ids)
    if full_img is None:
        return _image_not_found()
    log.debug("Full w=%d h=%d", full_img.getWidth(), full_img.getHeight())
    return _file_response(full_img, "full_image.fits")


def _image_cutout(_request, image_db_class, unit):
    """Get a raw image response from based on imput parameters.
    image_db_class should be the appropriate class (W13DeepCoadDb, W13RawDb, etc.)
    unit should be 'pixel' or 'arcsec'
    """
    ra = _request.args.get('ra')
    dec = _request.args.get('dec')
    filter = _request.args.get('filter')
    width = _request.args.get('width')
    height = _request.args.get('height')
    # check inputs
    try:
        ra, dec, filter = _assert_ra_dec_filter(ra, dec, filter, 'irg')
        try:
            width = float(width)
            height = float(height)
        except ValueError:
            msg = "INVALID_INPUT width={} height={}".format(width, height)
            raise ValueError(msg)
    except ValueError as e:
        return _error(ValueError.__name__, e.args[0], BAD_REQUEST)

    log.info("raw cutout pixel ra={} dec={} filter={} width={} height={}".format(
             ra, dec, filter, width, height))
    # fetch the image here
    img_getter = image_open(image_db_class, current_app.config)
    img = img_getter.image_cutout(ra, dec, filter, width, height, unit)
    if img is None:
        return _image_not_found()
    log.debug("Sub w={} h={}".format(img.getWidth(), img.getHeight()))
    return _file_response(img, "cutout.fits")


def _image_cutout_from_science_id(_request, image_db_class, science_id):
    """Get cutout image from the id given.
    image_db_class should be the appropriate class (W13CalexpDb, W13DeepCoadDb, W13RawDb, etc.)
    Units: arcsec, pixel (request parameters)
    """
    # fetch the interested parameters
    # Only one of (widthAng, heightAng),(widthPix, heightPix) should be valid
    params = ['ra', 'dec', 'widthAng', 'heightAng', 'widthPix', 'heightPix']
    ra, dec, widthAng, heightAng, widthPix, heightPix = [_request.args.get(p) for p in params]

    try:
        if widthAng is not None and heightAng is not None:
            sId, ra, dec, width, height, unit = _assert_cutout_parameters(
                    science_id, ra, dec, widthAng, heightAng, 'arcsec')
        elif widthPix is not None and heightPix is not None:
            sId, ra, dec, width, height, unit = _assert_cutout_parameters(
                science_id, ra, dec, widthPix, heightPix, 'pixel')
        else:
            msg = "INVALID_INPUT no dimensions for cutout specified"
            raise ValueError(msg)
    except ValueError as e:
        return _error(ValueError.__name__, e.args[0], BAD_REQUEST)
    # fetch the image here
    img_getter = image_open(image_db_class, current_app.config)
    # need to pass the source science id as string
    img = img_getter.imagecutout_from_science_id(science_id, ra, dec, width, height, unit)
    if img is None:
        return _image_not_found()
    log.debug("Sub w={} h={}".format(img.getWidth(), img.getHeight()))
    return _file_response(img, "cutout.fits")


# this will handle something like:
# GET /image/v0/skymap/deepCoadd/cutout?ra=19.36995&dec=-0.3146&filter=r&width=115&height=235
@imageREST.route('/skymap/deepCoadd/cutout', methods=['GET'])
def getISkyMapDeepCoaddCutout():
    """Get a stitched together deepCoadd image from /lsst/releaseW13EP deepCoadd_skyMap
    where width and height are in arcsecs.
    """
    return _stiched_image_cutout(request, 'arcsec')


# this will handle something like:
# GET /image/v0/skymap/deepCoadd/cutoutPixel?ra=19.36995&dec=-0.3146&filter=r&width=115&height=235
@imageREST.route('/skymap/deepCoadd/cutoutPixel', methods=['GET'])
def getISkyMapDeepCoaddCutoutPixel():
    """Get a stitched together deepCoadd image from /lsst/releaseW13EP deepCoadd_skyMap
    where width and height are in pixels.
    :query float ra: ra
    :query float dec: dec
    :query string filter: Filter
    :query integer width: Width
    :query integer height: Height
    :query string source: Optional filesystem path to provide imgserv
    """
    return _stiched_image_cutout(request, 'pixel')


def _stiched_image_cutout(_request, unit):
    """Get a stitched together deepCoadd image from /lsst/releaseW13EP deepCoadd_skyMap
    """
    source = _request.args.get("source", None)
    if not source:
        # Use a default
        source = current_app.config["DAX_IMG_DATASOURCE"]+"coadd/"
    # Be safe and encode source to utf8, just in case
    source = source.encode('utf8')
    log.debug("Using filesystem source: " + source)
    map_type = "deepCoadd_skyMap"
    ra = _request.args.get('ra')
    dec = _request.args.get('dec')
    filter = _request.args.get('filter')
    width = _request.args.get('width')
    height = _request.args.get('height')
    # check inputs - Many valid filter names are unknown and can't be checked.
    try:
        ra, dec = _assert_ra_dec(ra, dec)
        try:
            width = float(width)
            height = float(height)
        except ValueError:
            msg = "INVALID_INPUT width={} height={}".format(width, height)
            raise ValueError(msg)
    except ValueError as e:
        return _error(ValueError.__name__, e.args[0], BAD_REQUEST)

    log.info("skymap cutout pixel ra={} dec={} filter={} width={} height={}".format(
            ra, dec, filter, width, height))
    # fetch the image here
    ra_angle = afw_geom.Angle(ra, afw_geom.degrees)
    dec_angle = afw_geom.Angle(dec, afw_geom.degrees)
    center_coord = afw_coord.Coord(ra_angle, dec_angle, 2000.0)
    try:
        expo = getSkyMap(center_coord, int(width), int(height),
                         filter, unit, source, map_type)
    except RuntimeError as e:
        return _error("RuntimeError", e.message, INTERNAL_SERVER_ERROR)
    if expo is None:
        return _image_not_found()
    return _file_response(expo, "cutout.fits")


def _image_not_found(message=None):
    # HTTP 404 - NOT FOUND, RFC2616, Section 10.4.5
    if not message:
        message = "Image Not Found"
    response = jsonify({"exception": IOError.__name__, "message": message})
    response.status_code = NOT_FOUND  # ValueError == BAD REQUEST
    return response


def _error(exception, message, status_code):
    response = jsonify({"exception": exception, "message": message})
    response.status_code = status_code
    return response


def _file_response(img, file_name):
    tmp_path = tempfile.mkdtemp()
    file_path = os.path.join(tmp_path, file_name)
    log.debug("temporary file_path=%s", file_path)
    img.writeFits(file_path)
    resp = _make_file_response(file_path)
    os.remove(file_path)
    os.removedirs(tmp_path)
    return resp


def _make_file_response(file_name):
    # It would be nice to just write to 'data' instead of making a file.
    # writeFits defined in afw/python/lsst/afw/math/background.py
    # Using a cache of files might be desirable. We would need consistent and
    # unique identifiers for the files.
    try:
        with open(file_name, 'rb') as f:
            data = f.read()
            f.close()
            response = make_response(data)
            response.headers["Content-Disposition"] = "attachment; filename=image.fits"
            response.headers["Content-Type"] = "image/fits"
        return response
    except IOError as e:
        return _error(IOError.__name__, e.message, INTERNAL_SERVER_ERROR)

