#!/usr/b:in/env python

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

from flask import Blueprint, make_response, request, current_app

import lsst.afw.coord as afwCoord
import lsst.afw.geom as afwGeom
import lsst.log as log

from httplib import BAD_REQUEST, INTERNAL_SERVER_ERROR
from .locateImage import dbOpen, W13DeepCoaddDb, W13RawDb, W13CalexpDb
from .skymapStitch import getSkyMap

imageREST = Blueprint('imageREST', __name__, template_folder='imgserv')


# this will eventually print list of supported versions
@imageREST.route('/')
def index():
    return """
Hello, LSST Image Cutout Service here. Try something like:<br />
/image/v0/raw?ra=1&dec=1&filter=r<br />
/image/v0/raw/cutout?ra=1&dec=1&filter=r&width=12&height=12
"""


def checkRaDecFilter(raIn, decIn, filt, validFilters):
    '''Returns: valid, ra, dec, filt, msg  where:
      valid is true if the inputs were accetpable.
      ra and dec are the floating point equivalants of raIn and decIn.
      msg is and error message if valid is false, otherwise blank.
    '''
    # @todo throw exception instead of return valid DM-1980
    valid, ra, dec, msg = checkRaDec(raIn, decIn)
    if filt not in validFilters:
        msg = ("Invalid filter {}. valid filters are {}.".format(filt,
               validFilters))
        valid = False
    return valid, ra, dec, filt, msg


def checkRaDec(raIn, decIn):
    '''Returns: valid, ra, dec, msg  where:
      valid is true if the inputs were accetpable.
      ra and dec are the floating point equivalants of raIn and decIn.
      msg is and error message if valid is false, otherwise blank.
    '''
    # @todo throw exception instead of return valid DM-1980
    ra = 0.0
    dec = 0.0
    valid = True
    msg = ""
    try:
        ra = float(raIn)
        dec = float(decIn)
    except ValueError:
        msg = "NEED_HTTP INVALID_INPUT ra={} dec={}".format(raIn, decIn)
        valid = False
    return valid, ra, dec, msg



def checkParamsICutoutFromScienceId(scienceId, raIn, decIn,
                                    width, height, units):
    ''' Check and convert request parameters to numeric values.
    '''
    valid, ra, dec, msg = checkRaDec(raIn, decIn)
    try:
        if units == 'arcsecond':
            width = float(width)
            height = float(height)
        elif units == 'pixel':
            width = int(width)
            height = int(height)
        else:
            msg = "No Units specified for cutout dimensions"
            valid = False
        scId = int(scienceId)
    except ValueError:
        msg = ("NEED_HTTP INVALID_INPUT scienceId={} width={} height={}"
               .format(scienceId, width, height))
        valid = False
    return valid, scId, ra, dec, width, height, units, msg


def getRequestParams(_request, params):
    '''Returns the values of the specified parameters specified in the
       params as string array.
    '''
    vals = []
    for p in params:
        vals.append(_request.args.get(p))
    return vals


# this will handle something like:
# GET /image/v0/raw?ra=359.195&dec=-0.1055&filter=r
@imageREST.route('/raw', methods=['GET'])
def getRaw():
    return _getIFull(request, W13RawDb)

# this will handle something like:
# GET /image/v0/raw/ids?run=5646&camcol=4&field=694&filter=g
@imageREST.route('/raw/ids', methods=['GET'])
def getIRawIds():
    return _getIIds(request, W13RawDb)

# this will handle something like:
# GET /image/v0/raw/id?id=3325410171
# Which should translate to run=3325 camcol=1 field=171 filter=z
@imageREST.route('/raw/id', methods=['GET'])
def getIRawScienceId():
    return _getIScienceId(request, W13RawDb)

# this will handle something like:
# GET /image/v0/raw/cutout?ra=359.195&dec=-0.1055&filter=r&width=30.0&height=45.0
@imageREST.route('/raw/cutout', methods=['GET'])
def getIRawCutout():
    return _getICutout(request, W13RawDb, 'arcsecond')

# this will handle something like:
# GET /image/v0/raw/cutoutPixel?ra=359.195&dec=-0.1055&filter=r&width=30.0&height=45.0
@imageREST.route('/raw/cutoutPixel', methods=['GET'])
def getIRawCutoutPixel():
    return _getICutout(request, W13RawDb, 'pixel')

# this will handle something like:
# GET /image/v0/calexp?filter=r&ra=37.644598&dec=0.104625
@imageREST.route('/calexp', methods=['GET'])
def getCalexp():
    return _getIFull(request, W13CalexpDb)

# this will handle something like:
# GET /image/v0/calexp/ids?run=5646&camcol=4&field=694&filter=g
@imageREST.route('/calexp/ids', methods=['GET'])
def getICalexpIds():
    return _getIIds(request, W13CalexpDb)

# this will handle something like:
# GET /image/v0/calexp/id?id=3325410171
# Which should translate to run=3325 camcol=1 field=171 filter=z
@imageREST.route('/calexp/id', methods=['GET'])
def getICalexpScienceId():
    return _getIScienceId(request, W13CalexpDb)

# this will handle something like:
# GET /image/v0/calexp/cutout?ra=37.644598&dec=0.104625&filter=r&width=30.0&height=45.0
@imageREST.route('/calexp/cutout', methods=['GET'])
def getICalexpCutout():
    return _getICutout(request, W13CalexpDb, 'arcsecond')


# this will handle something like:
# GET /image/v0/calexp/cutoutPixel?ra=37.644598&dec=0.104625&filter=r&width=30.0&height=45.0
@imageREST.route('/calexp/cutoutPixel', methods=['GET'])
def getICalexpCutoutPixel():
    return _getICutout(request, W13CalexpDb, 'pixel')


# this will handle something like:
# GET /image/v0/calexp/5646240694/cutout?ra=37.6292&dec=-0.0658&widthAng=30.0&heightAng=45.0
# GET /image/v0/calexp/5646240694/cutout?ra=37.6292&dec=-0.0658&widthPix=30&heightPix=45
@imageREST.route('/calexp/<Id>/cutout', methods=['GET'])
def getICalexpCutoutFromScienceId(Id):
    return _getICutoutFromScienceId(request, W13CalexpDb, Id)


# this will handle something like:
# GET /image/v0/deepCoadd?ra=19.36995&dec=-0.3146&filter=r
@imageREST.route('/deepCoadd', methods=['GET'])
def getDeepCoadd():
    return _getIFull(request, W13DeepCoaddDb)

# this will handle something like:
# GET /image/v0/deepCoadd/ids?tract=0&patch=225,1&filter='i'
@imageREST.route('/deepCoadd/ids', methods=['GET'])
def getIDeepCoaddsIds():
    return _getIIds(request, W13DeepCoaddDb)

# this will handle something like:
# GET /image/v0/deepCoadd/id?id=23986176
# Which should translate to tract= patch=1 filter=
@imageREST.route('/deepCoadd/id', methods=['GET'])
def getIDeepCoaddScienceId():
    return _getIScienceId(request, W13DeepCoaddDb)

# this will handle something like:
# GET /image/v0/deepCoadd/cutout?ra=19.36995&dec=-0.3146&filter=r&width=115&height=235
@imageREST.route('/deepCoadd/cutout', methods=['GET'])
def getIDeepCoaddCutout():
    return _getICutout(request, W13DeepCoaddDb, 'arcsecond')


# this will handle something like:
# GET /image/v0/deepCoadd/cutoutPixel?ra=19.36995&dec=-0.3146&filter=r&width=115&height=235
@imageREST.route('/deepCoadd/cutoutPixel', methods=['GET'])
def getIDeepCoaddCutoutPixel():
    return _getICutout(request, W13DeepCoaddDb, 'pixel')

def _getIFull(_request, W13db):
    ''' Get a full image from the input paramters.
    W13db should be the appropriate class (W13DeepCoadDb, W13RawDb, etc.)
    '''
    raIn = _request.args.get('ra')
    decIn = _request.args.get('dec')
    filt = _request.args.get('filter')
    filt = filt.encode('ascii')

    # check inputs
    valid, ra, dec, filt, msg = checkRaDecFilter(raIn, decIn, filt, 'irg')
    if not valid:
        # TODO: use HTTP errors DM-1980
        resp = "INVALID_INPUT {}".format(msg)
        return resp
    log.info("raw ra={} dec={} filt={}".format(ra, dec, filt))
    # fetch the image here
    w13db = dbOpen("~/.lsst/dbAuth-dbServ.ini", W13db)
    imgFull = w13db.getImageFull(ra, dec, filt)
    if imgFull is None:
        return _imageNotFound()
    log.debug("Full w=%d h=%d", imgFull.getWidth(), imgFull.getHeight())
    tmpPath = tempfile.mkdtemp()
    fileName = os.path.join(tmpPath, "fullImage.fits")
    log.info("temporary fileName=%s", fileName)
    imgFull.writeFits(fileName)
    resp = responseFile(fileName)
    os.remove(fileName)
    os.removedirs(tmpPath)
    return resp

def _getIIds(_request, W13db):
    ''' Get a full image from the field ids given.
    W13db should be the appropriate class (W13DeepCoadDb, W13RawDb, etc.)
    '''
    # fetch the image here
    w13db = dbOpen("~/.lsst/dbAuth-dbServ.ini", W13db)
    validIds = False
    ids = {}
    ids, validIds = w13db.getIdsFromRequest(_request)
    log.info("valid={} id {}".format(validIds, ids))
    if not validIds:
        resp = "INVALID_INPUT {}".format(ids)
        return resp
    imgFull, butler = w13db.getImageByIds(ids)
    if imgFull is None:
        return _imageNotFound()
    log.debug("Full w=%d h=%d", imgFull.getWidth(), imgFull.getHeight())
    tmpPath = tempfile.mkdtemp()
    fileName = os.path.join(tmpPath, "fullImage.fits")
    log.info("temporary fileName=%s", fileName)
    imgFull.writeFits(fileName)
    resp = responseFile(fileName)
    os.remove(fileName)
    os.removedirs(tmpPath)
    return resp

def _getIScienceId(_request, W13db):
    ''' Get a full image from the id given.
    W13db should be the appropriate class (W13DeepCoadDb, W13RawDb, etc.)
    '''
    w13db = dbOpen("~/.lsst/dbAuth-dbServ.ini", W13db)
    value = request.args.get("id")
    if value is None:
        resp = "INVALID_INPUT value={}".format(value)
        return resp
    ids, valid = w13db.getImageIdsFromScienceId(value)
    log.info("valid={} value={} ids{}".format(valid, value, ids))
    print("valid={} value={} ids{}".format(valid, value, ids))
    if not valid:
        resp = "INVALID_INPUT value={} {}".format(value, ids)
        return resp
    imgFull, butler = w13db.getImageByIds(ids)
    if imgFull is None:
        return _imageNotFound()
    log.debug("Full w=%d h=%d", imgFull.getWidth(), imgFull.getHeight())
    tmpPath = tempfile.mkdtemp()
    fileName = os.path.join(tmpPath, "fullImage.fits")
    log.info("temporary fileName=%s", fileName)
    imgFull.writeFits(fileName)
    resp = responseFile(fileName)
    os.remove(fileName)
    os.removedirs(tmpPath)
    return resp

def _getICutout(_request, W13db, units):
    '''Get a raw image from based on imput parameters.
    W13db should be the appropriate class (W13DeepCoadDb, W13RawDb, etc.)
    units should be 'pixel' or 'arcsecond'
    '''
    raIn = _request.args.get('ra')
    decIn = _request.args.get('dec')
    filt = _request.args.get('filter')
    widthIn = _request.args.get('width')
    heightIn = _request.args.get('height')
    # check inputs
    valid, ra, dec, filt, msg = checkRaDecFilter(raIn, decIn, filt, 'irg')
    if not valid:
        return _error(ValueError.__name__, msg, BAD_REQUEST)
    try:
        width = float(widthIn)
        height = float(heightIn)
    except ValueError:
        msg = "INVALID_INPUT width={} height={}".format(widthIn, heightIn)
        return _error(ValueError.__name__, msg, BAD_REQUEST)
    log.info("raw cutout pixel ra={} dec={} filt={} width={} height={}".format(
             ra, dec, filt, width, height))

    # fetch the image here
    w13db = dbOpen("~/.lsst/dbAuth-dbServ.ini", W13db)
    img = w13db.getImage(ra, dec, filt, width, height, units)
    if img is None:
        return _imageNotFound()
    log.debug("Sub w={} h={}".format(img.getWidth(), img.getHeight()))
    tmpPath = tempfile.mkdtemp()
    fileName = os.path.join(tmpPath, "cutout.fits")
    log.info("temporary fileName=%s", fileName)
    img.writeFits(fileName)
    resp = responseFile(fileName)
    os.remove(fileName)
    os.removedirs(tmpPath)
    return resp


def _getICutoutFromScienceId(_request, W13db, scienceId):
    '''Get cutoff of calexp image from the id given.
    W13db presumed to be W13CalexpDb.
    Units: arcsecond, pixel (to be inferred from the request parameters)
    '''
    # fetch the interested parameters
    # Only one of (widthAng, heightAng),(widthPix, heightPix) should be valid
    params = ['ra', 'dec', 'widthAng', 'heightAng', 'widthPix', 'heightPix']
    raIn, decIn, widthAng, heightAng, widthPix, heightPix = getRequestParams(_request, params)

    valid, units, msg = False, "", ""
    width, height = 0.0, 0.0
    if (widthAng is not None and heightAng is not None):
        valid, sId, ra, dec, width, height, units, msg = checkParamsICutoutFromScienceId(
                scienceId, raIn, decIn, widthAng, heightAng, 'arcsecond')
    elif (widthPix is not None and heightPix is not None):
        valid, sId, ra, dec, width, height, units, msg = checkParamsICutoutFromScienceId(
                scienceId, raIn, decIn, widthPix, heightPix, 'pixel')
    else:
        msg = "INVALID_INPUT no dimensions for cutout specified"
    if not valid:
        return _error(ValueError.__name__, msg, BAD_REQUEST)
    # fetch the image here
    w13db = dbOpen("~/.lsst/dbAuth-dbServ.ini", W13db)
    # need to pass the source science id as string
    img = w13db._getImageCutoutFromScienceId(scienceId, ra, dec, width, height, units)
    if img is None:
        return _imageNotFound()
    log.debug("Sub w={} h={}".format(img.getWidth(), img.getHeight()))
    tmpPath = tempfile.mkdtemp()
    fileName = os.path.join(tmpPath, "cutout.fits")
    log.info("temporary fileName=%s", fileName)
    img.writeFits(fileName)
    resp = responseFile(fileName)
    os.remove(fileName)
    os.removedirs(tmpPath)
    return resp


# this will handle something like:
# GET /image/v0/skymap/deepCoadd/cutout?ra=19.36995&dec=-0.3146&filter=r&width=115&height=235
@imageREST.route('/skymap/deepCoadd/cutout', methods=['GET'])
def getISkyMapDeepCoaddCutout():
    '''Get a stitched together deepCoadd image from /lsst/releaseW13EP deepCoadd_skyMap
    where width and height are in arcseconds.
    :query float ra: ra
    :query float dec: dec
    :query string filter: Filter
    :query integer width: Width
    :query integer height: Height
    :query string source: Optional filesystem path to provide imgserv
    '''
    return _getISkyMapDeepCoaddCutout(request, 'arcsecond')


# this will handle something like:
# GET /image/v0/skymap/deepCoadd/cutoutPixel?ra=19.36995&dec=-0.3146&filter=r&width=115&height=235
@imageREST.route('/skymap/deepCoadd/cutoutPixel', methods=['GET'])
def getISkyMapDeepCoaddCutoutPixel():
    '''Get a stitched together deepCoadd image from /lsst/releaseW13EP deepCoadd_skyMap
    where width and height are in pixels.
    :query float ra: ra
    :query float dec: dec
    :query string filter: Filter
    :query integer width: Width
    :query integer height: Height
    :query string source: Optional filesystem path to provide imgserv
    '''
    return _getISkyMapDeepCoaddCutout(request, 'pixel')


def _getISkyMapDeepCoaddCutout(_request, units):
    '''Get a stitched together deepCoadd image from /lsst/releaseW13EP deepCoadd_skyMap
    '''
    source = _request.args.get("source", None)
    if not source:
        # Use a default
        source = current_app.config["dax.imgserv.default_source"]
        source = "/datasets/sdss/preprocessed/dr7/sdss_stripe82_00/coadd/"  # TODO

    # Be safe and encode source to utf8, just in case
    source = source.encode('utf8')
    log.debug("Using filesystem source: " + source)

    mapType = "deepCoadd_skyMap"
    patchType = "deepCoadd"

    raIn = _request.args.get('ra')
    decIn = _request.args.get('dec')
    filt = _request.args.get('filter')
    widthIn = _request.args.get('width')
    heightIn = _request.args.get('height')
    # check inputs - Many valid filter names are unknown and can't be checked.
    valid, ra, dec, msg = checkRaDec(raIn, decIn)
    if not valid:
        msg = "INVALID_INPUT {}".format(msg)
        return _error(ValueError.__name__, msg, BAD_REQUEST)
    try:
        width = float(widthIn)
        height = float(heightIn)
        # The butler isn't fond of unicode in this case.
        filt = filt.encode('ascii')
    except ValueError:
        msg = "INVALID_INPUT width={} height={}".format(widthIn, heightIn)
        return _error(ValueError.__name__, msg, BAD_REQUEST)
    log.info("skymap cutout pixel ra={} dec={} filt={} width={} height={}".format(
            ra, dec, filt, width, height))
    # fetch the image here
    raA = afwGeom.Angle(ra, afwGeom.degrees)
    decA = afwGeom.Angle(dec, afwGeom.degrees)
    ctrCoord = afwCoord.Coord(raA, decA, 2000.0)
    try:
        expo = getSkyMap(ctrCoord, int(width), int(height), filt, units, source, mapType, patchType)
    except RuntimeError as e:
        return _error("RuntimeError", e.message, INTERNAL_SERVER_ERROR)
    if expo is None:
        return _imageNotFound()
    tmpPath = tempfile.mkdtemp()
    fileName = os.path.join(tmpPath, "cutout.fits")
    log.info("temporary fileName=%s", fileName)
    expo.writeFits(fileName)
    resp = responseFile(fileName)
    os.remove(fileName)
    os.removedirs(tmpPath)
    return resp


def _imageNotFound():  # FIXME: Not sure what error this should be, 400, 404, 500?
    return _error("ImageNotFound", "Image Not Found", INTERNAL_SERVER_ERROR)


def _error(exception, message, status_code):
    return make_response({"exception": exception, "message": message}, status_code)


def responseFile(fileName):
    # It would be nice to just write to 'data' instead of making a file.
    # writeFits defined in afw/python/lsst/afw/math/background.py
    # Using a cache of files might be desirable. We would need consistent and
    # unique identifiers for the files.
    try:
        with open(fileName, 'r') as f:
            data = f.read()
            f.close()
            response = make_response(data)
            response.headers["Content-Disposition"] = "attachment; filename=image.fits"
            response.headers["Content-Type"] = "image/fits"
        return response
    except IOError as e:
        return _error(IOError.__name__, e.message, INTERNAL_SERVER_ERROR)
