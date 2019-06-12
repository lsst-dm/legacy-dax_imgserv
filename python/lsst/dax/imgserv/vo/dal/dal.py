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
from abc import ABC

from flask import render_template, url_for, request

"""
This module provides REST implementation of DALI/VOSI for resources as defined
by IVOA Specification:
    http://www.ivoa.net/documents/DALI/20170517/REC-DALI-1.1.html

Required Python version: 3.6.5+
"""

DAL_Service= {
    "resource_type": ["name", "required?", "template_file"],
    "DALI-sync": ["/sync", "service-specific", ""],
    "DALI-async": ["/async", "service-specific", ""],
    "DALI-examples": ["/examples", "no", "./dal_examples.xml"],
    "VOSI-availability": ["/availability", "yes", "./dal_availability.xml"],
    "VOSI-capabilities": ["/capabilities", "yes", "./dal_capabilities.xml"],
    "VOSI-tables": ["/tables", "service-specific", "./dal_tables.xml"]
}

STD_PARAMETERS = {
    "parameter_type": ["defined?", "case-sensitive?", "mult-valued?", "ref"],
    "REQUEST": [False, None, None, None],
    "VERSION": [False, None, None, None],
    "RESPONSEFORMAT": [True, True, False, "#RESPONSEFORMAT"],
    "MAXREC": [True, True, False, None],
    "UPLOAD": [True, True, True, None],
    "RUNID": [True, True, False, None]
}

RESPONSEFORMAT = {
    "RF1": ["table type", "media type", "short form"],
    "RF2": ["VOTable", "application/x-votable+xml", "votable"],
    "RF3": ["VOTable", "text/xml", "votable"],
    "RF4": ["comma-separated values", "text/csv", "csv"],
    "RF5": ["tab separated values", "text/tab-separated-values", "tsv"],
    "RF6": ["FITS file", "application/fits", "fits"],
    "RF7": ["pretty-printed text", "text/plain", "text"],
    "RF8": ["pretty-printed Web page", "text/html", "html"]
}


class DAL(ABC):
    """ Basic DAL service for providing VOSI/DALI operations.
    """
    def handle_dali(self, params: dict) -> str:
        """ Handle DALI/VOSI operations.

        Parameters
        ----------
        params : `dict`
            the HTTP parameters.

        Returns
        -------
        response: `str`
            the response in xml.
        """
        raise NotImplemented("DAL.handle_dali()")

    def do_sync(self, params: dict) -> object:
        """ Perform a sync operation.

        Parameters
        ----------
        params : `dict`
            the parameters.

        Returns
        -------
        response: `object`
            the response in xml.
        """
        raise NotImplemented("DAL.do_sync()")

    def do_async(self, params: dict) -> object:
        """ Perform an async operation.

        Parameters
        ----------
        params : `dict`
            the request parameters.

        Returns
        -------
        response: `str`
            the response in xml.

        """
        raise NotImplemented("DAL.do_async()")

    def get_examples(self, params: dict) -> str:
        """ Get the examples for this service.

        Parameters
        ----------
        params : `dict`
            the request parameters.

        Returns
        -------
        response: `str`
            the response in xml.

        """
        resp = render_template("dali_examples.xml")
        return resp

    def get_availability(self, params: dict) -> str:
        """ Get service availability status.

        Parameters
        ----------
        params : `dict`
            the request parameters.

        Returns
        -------
        response: `str`
            the response in xml.

        """
        resp = render_template("vosi_avail.xml",
                               service_status=params["status"],
                               dal_service_name=params["service_name"])
        return resp

    def get_capabilities(self, params: dict) -> str:
        """ Get the service capabilities.

        Parameters
        ----------
        params : `dict`
            the request parameters.

        Returns
        -------
        response: `str`
            the response in xml.

        """
        resp = render_template("vosi_capabilities.xml")
        return resp
