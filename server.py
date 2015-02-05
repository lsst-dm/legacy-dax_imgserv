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

@author  Jacek Becla, SLAC, SLAC
"""

from flask import Flask
from flask import request

app = Flask(__name__)


curVerImg = 0 # version of the API for /image

@app.route('/')
def index():
    return """
Hello, LSST Image Cutout Service here. Try something like:<br />
/image/v%d/raw?ra=1&decl=1&filter=r<br />
/image/v%d/raw/cutout?ra=1&decl=1&filter=r&width=12&height=12
""" % (curVerImg, curVerImg)


# Print list of supported versions for /image
@app.route('/image', methods=['GET'])
def getI():
    return "v%d" % curVerImg

# this will handle something like:
# GET /image/v0/raw?ra=1&decl=1&filter=r
@app.route('/image/v%d/raw' % curVerImg, methods=['GET'])
def getI_curVer_raw():
    ra = request.args.get('ra', '1')
    decl = request.args.get('decl', '1')
    filter = request.args.get('filter', 'r')

    # fetch the image here

    return "the raw image for a given ra/dec/filter"

# this will handle something like:
# GET /image/v0/raw/cutout?ra=1&decl=1&filter=r&width=12&height=12
@app.route('/image/v%d/raw/cutout' % curVerImg, methods=['GET'])
def getI_curVer_raw_cutout():
    print request.args
    ra = request.args.get('ra', '1')
    print "I got ra!: %s" % (ra)
    decl = request.args.get('decl', '1')
    filter = request.args.get('filter', 'r')
    width = request.args.get('width', '10')
    height = request.args.get('height', '10')

    # fetch the image here

    return "the cutout of raw image for ra=%s, decl=%s, filter=%s" % \
        (ra, decl, filter)


##### Main #########################################################################
if __name__ == '__main__':
    app.run(debug=True)
