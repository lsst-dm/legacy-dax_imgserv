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
This is a program for running RESTful LSST Image Cutout Server (only).
Use it for tests. It is really meant to run as part of the central
Web Service, e.g., through webserv/bin/server.py

@author  Jacek Becla, SLAC
"""

import sys
from flask import Flask
from lsst.dax.imgserv import imageREST_v0

app = Flask(__name__)

app.register_blueprint(imageREST_v0.imageREST, url_prefix='/image')
app.config["dax.imgserv.default_source"] = "/lsst7/releaseW13EP"

if __name__ == '__main__':
    try:
        app.run(debug=True)
    except Exception, e:
        print "Problem starting the server.", str(e)
        sys.exit(1)
