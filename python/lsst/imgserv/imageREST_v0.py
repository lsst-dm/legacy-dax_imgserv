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

@author  Jacek Becla, SLAC
"""
import os
import uuid

from flask import Blueprint, make_response, request

from lsst.imgserv.locateImage import dbOpen

imageREST = Blueprint('imageREST', __name__, template_folder='imgserv')

# this will eventually print list of supported versions
@imageREST.route('/')
def index():
    return """
Hello, LSST Image Cutout Service here. Try something like:<br />
/image/v0/raw?ra=1&dec=1&filter=r<br />
/image/v0/raw/cutout?ra=1&dec=1&filter=r&width=12&height=12
"""


# this will handle something like:
# GET /image/v0/raw?ra=1&dec=1&filter=r
@imageREST.route('/raw', methods=['GET'])
def getRaw():
    ra = request.args.get('ra', '1')
    dec = request.args.get('dec', '1')
    filter = request.args.get('filter', 'r')

    # fetch the image here
    # TODO - check inputs are valid.
    ra = float(ra)
    dec = float(dec)
    print "raw ra={} dec={} filter={}".format(ra, dec, filter)
    # fetch the image here
    w13Raw = dbOpen("~/.lsst/dbAuth-dbServ.txt")
    imgFull = w13Raw.getImageFull(ra, dec)
    print "Full w={} h={}".format(imgFull.getWidth(), imgFull.getHeight())
    fileName = str(uuid.uuid4())
    imgFull.writeFits(fileName)
    w13Raw.closeConnection()
    resp = responseFile(fileName)
    os.remove(fileName)
    return resp

# this will handle something like:
# GET /image/v0/raw/cutout?ra=1&dec=1&filter=r&width=12&height=12
@imageREST.route('/raw/cutout', methods=['GET'])
def getIRawCutout():
    print request.args
    ra = float(request.args.get('ra', '1'))
    dec = float(request.args.get('dec', '1'))
    filter = request.args.get('filter', 'r')
    width = float(request.args.get('width', '10'))
    height = float(request.args.get('height', '10'))
    print "raw cutout ra={} dec={} filter={} width={} height={}".format(
        ra, dec, filter, width, height)

    # fetch the image here
    w13Raw = dbOpen("~/.lsst/dbAuth-dbServ.txt")
    img = w13Raw.getImage(ra, dec, width, height)
    print "Sub w={} h={}".format(img.getWidth(), img.getHeight())
    fileName = str(uuid.uuid4())
    img.writeFits(fileName)
    w13Raw.closeConnection()
    resp = responseFile(fileName)
    os.remove(fileName)
    return resp

def responseFile(fileName):
    # It would be nice to just write to 'data' instead of making a file.
    # writeFits defined in afw/python/lsst/afw/math/background.py
    response = "/image/raw  failed"
    with open(fileName, 'r') as f:
        data = f.read()
        f.close()
        response = make_response(data)
        response.headers["Content-Disposition"] = "attachment; filename=image.fits"
    return response
