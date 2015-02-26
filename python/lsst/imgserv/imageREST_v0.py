#!/usr/bin/python

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

@author  Jacek Becla, SLAC; John Gates, SLAC
"""
import os
import tempfile
import uuid

from flask import Blueprint, make_response, request

import lsst.log as log

from lsst.imgserv.locateImage import dbOpen, W13DeepCoaddDb, W13RawDb

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
    ra = 0.0
    dec = 0.0
    valid = True
    msg = ""
    try:
        ra = float(raIn)
        dec = float(decIn)
    except ValueError as e:
        msg = "NEED_HTTP INVALID_INPUT ra={} dec={}".format(raIn, decIn)
        valid = False
    if filt not in validFilters:
        msg = "Invalid filter {}. valid filters are {}.".format(filt, validFilters)
        valid = False
    return valid, ra, dec, filt, msg

# this will handle something like:
# GET /image/v0/raw?ra=359.195&dec=-0.1055&filter=r
@imageREST.route('/raw', methods=['GET'])
def getRaw():
    return _getIFull(request, W13RawDb)

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
# GET /image/v0/deepCoadd?ra=19.36995&dec=-0.3146&filter=r
@imageREST.route('/deepCoadd', methods=['GET'])
def getDeepCoadd():
    return _getIFull(request, W13DeepCoaddDb)

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

def _getIFull(request, W13db):
    ''' Get a full image from the input paramters.
    W13db should be the appropriate class (W13DeepCoadDb, W13RawDb, etc.)
    '''
    raIn = request.args.get('ra')
    decIn = request.args.get('dec')
    filt = request.args.get('filter')

    # check inputs
    valid, ra, dec, filt, msg = checkRaDecFilter(raIn, decIn, filt, 'irg')
    if not valid:
        # TODO: use HTTP errors DM-1980
        resp = "INVALID_INPUT {}".format(msg)
        return resp
    log.info("raw ra={} dec={} filt={}".format(ra, dec, filt))
    # fetch the image here
    w13db = dbOpen("~/.lsst/dbAuth-dbServ.txt", W13db)
    imgFull = w13db.getImageFull(ra, dec)
    if imgFull == None:
        # TODO: use HTTP errors DM-1980
        return "image not found"
    log.debug("Full w=%d h=%d", imgFull.getWidth(), imgFull.getHeight())
    tmpPath = tempfile.mkdtemp()
    fileName = os.path.join(tmpPath, "fullImage.fits")
    log.info("temporary fileName=%s", fileName)
    imgFull.writeFits(fileName)
    w13db.closeConnection()
    resp = responseFile(fileName)
    os.remove(fileName)
    os.removedirs(tmpPath)
    return resp

def _getICutout(request, W13db, units):
    '''Get a raw image from based on imput parameters.
    W13db should be the appropriate class (W13DeepCoadDb, W13RawDb, etc.)
    units should be 'pixel' or 'arcsecond'
    '''
    raIn = request.args.get('ra')
    decIn = request.args.get('dec')
    filt = request.args.get('filter')
    widthIn = request.args.get('width')
    heightIn = request.args.get('height')
    # check inputs
    valid, ra, dec, filt, msg = checkRaDecFilter(raIn, decIn, filt, ('i', 'r', 'g'))
    if not valid:
        # TODO: use HTTP errors DM-1980
        resp = "INVALID_INPUT {}".format(msg)
        return resp
    try:
        width = float(widthIn)
        height = float(heightIn)
    except ValueError as e:
        # TODO: use HTTP errors DM-1980
        resp = "INVALID_INPUT width={} height={}".format(widthIn, heightIn)
        return resp
    log.info("raw cutout pixel ra={} dec={} filt={} width={} height={}".format(
            ra, dec, filt, width, height))

    # fetch the image here
    w13db = dbOpen("~/.lsst/dbAuth-dbServ.txt", W13db)
    img = w13db.getImage(ra, dec, width, height, units)
    if img == None:
        # TODO: use HTTP errors DM-1980
        return "image not found"
    log.debug("Sub w={} h={}".format(img.getWidth(), img.getHeight()))
    tmpPath = tempfile.mkdtemp()
    fileName = os.path.join(tmpPath, "cutout.fits")
    log.info("temporary fileName=%s", fileName)
    img.writeFits(fileName)
    w13db.closeConnection()
    resp = responseFile(fileName)
    os.remove(fileName)
    os.removedirs(tmpPath)
    return resp

def responseFile(fileName):
    # It would be nice to just write to 'data' instead of making a file.
    # writeFits defined in afw/python/lsst/afw/math/background.py
    # Using a cache of files might be desirable. We would need consistent and
    # unique identifiers for the files.
    response = ""
    try:
        with open(fileName, 'r') as f:
            data = f.read()
            f.close()
            response = make_response(data)
            response.headers["Content-Disposition"] = "attachment; filename=image.fits"
            response.headers["Content-Type"] = "image/fits"
    except IOError as e:
        # TODO: use HTTP errors DM-1980
        response = "system_error {}".format(e)
    return response
