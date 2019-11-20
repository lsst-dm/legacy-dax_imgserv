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

max_image_cutout_size = 9.6 # in squared degrees

config_json = dict({
  "DEBUG": True,
  "DAX_IMG_TEMPDIR": "/tmp/imageworker_results",
  "DAX_IMG_VALIDATE": True,
  "DAX_IMG_DS": "/datasets/sdss/preprocessed/dr7/sdss_stripe82_01",
  "DAX_IMG_TAB_SCICCDEXP": "Science_Ccd_Exposure",
  "DAX_IMG_TAB_DEEPCOADD": "DeepCoadd",
  "DAX_IMG_DR": "/datasets/sdss/preprocessed/dr7/runs",
  "DAX_IMG_BUTLER_KEYS1": ["run", "camcol", "field", "filter"],
  "DAX_IMG_COLUMNS1": ["run", "camcol", "field", "filterName"],
  "DAX_IMG_BUTLER_KEYS2": ["tract", "patch", "filter"],
  "DAX_IMG_COLUMNS2": ["tract", "patch", "filterName"],
  "DAX_IMG_BUTLER_POL0": "fpC",
  "DAX_IMG_BUTLER_POL1": "calexp",
  "DAX_IMG_BUTLER_POL2": "deepCoadd",
  "DAX_IMG_SKYMAP_DEEPCOADD": "deepCoadd_skyMap"
})

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


