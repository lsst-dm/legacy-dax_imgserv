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

from flask import Blueprint, Flask, request

imageREST = Blueprint('imageREST', __name__, template_folder='imgserv')

# this will eventually print list of supported versions
@imageREST.route('/')
def index():
    return """
LSST Image Cutout Service v0 here. Try something like:<br />
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

    return "the raw image for a given ra/dec/filter"

# this will handle something like:
# GET /image/v0/raw/cutout?ra=1&dec=1&filter=r&width=12&height=12
@imageREST.route('/raw/cutout', methods=['GET'])
def getIRawCutout():
    print request.args
    ra = request.args.get('ra', '1')
    print "I got ra!: %s" % (ra)
    dec = request.args.get('dec', '1')
    filter = request.args.get('filter', 'r')
    width = request.args.get('width', '10')
    height = request.args.get('height', '10')

    # fetch the image here

    return "the cutout of raw image for ra=%s, dec=%s, filter=%s" % \
        (ra, dec, filter)
