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
import os
from configparser import RawConfigParser

# 9.6 (unit in squared degrees) is the size of LSST's focal plane.
# See https://www.lsst.org/scientists/keynumbers
MAX_IMAGE_CUTOUT_SIZE = 9.6

config_datasets = {
    "ci_hsc": {
        "IMG_REPO_ROOT": "/datasets/ci_hsc_gen3/DATA",
        "IMG_DEFAULT_COLLECTION": "shared/ci_hsc_output",
        "IMG_OBSCORE_DB": "ci_hsc_db",
        "IMG_SCHEMA_TABLE": "imgserv.obscore",
        "IMG_DEFAULT_FILTER": "r",
        "INSTRUMENT": "HSC",
        "calexp": ["instrument", "detector", "visit"],
        "raw": ["instrument", "detector", "exposure"],
        "deepCoadd": ["band", "skymap",  "tract", "patch"]
    },
    "hsc_rc2": {
        "IMG_REPO_ROOT": "/project/validation_hsc_gen3/",
        "IMG_DEFAULT_COLLECTION": "shared/valid_hsc_all",
        "IMG_OBSCORE_DB": "lsstdb1",
        "IMG_SCHEMA_TABLE": "imgserv.obscore",
        "IMG_DEFAULT_FILTER": "r",
        "INSTRUMENT": "HSC",
        "calexp": ["instrument", "detector", "visit"],
        "raw": ["instrument", "detector", "exposure"],
        "deepCoadd": ["band", "skymap",  "tract", "patch"]
    },
    "default": "hsc_rc2"
}

# load webserv.ini configuration
tasks_parser = RawConfigParser()
tasks_parser.optionxform = str

defaults_file = os.environ.get("WEBSERV_CONFIG", "~/.lsst/webserv.ini")
try:
    with open(os.path.expanduser(defaults_file)) as cfg:
        tasks_parser.read_file(cfg, source=defaults_file)
        # provide access to webserv settings
        webserv_config = dict(tasks_parser.items("webserv"))
except FileNotFoundError:
    # webserv settings not provided
    webserv_config = dict({})


