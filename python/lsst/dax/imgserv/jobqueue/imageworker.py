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
import tempfile
import os
import json
from configparser import RawConfigParser

from celery import Celery

from flask import current_app
# use following format for circular reference
import lsst.dax.imgserv.vo.imageSODA as imageSODA

defaults_file = os.environ.get("WEBSERV_CONFIG", "~/.lsst/webserv.ini")
# Initialize configuration
tasks_parser = RawConfigParser()
tasks_parser.optionxform = str

with open(os.path.expanduser(defaults_file)) as cfg:
    tasks_parser.read_file(cfg, defaults_file)

webserv_config = dict(tasks_parser.items("webserv"))

# initialize app_celery outside flask
app_celery = Celery("Queue Worker")
app_celery.config_from_object('etc.celery.celery_config')


def make_celery():
    """
        Initiate the celery app for client access.
    """
    global app_celery
    app = current_app
    app_celery = Celery(app.import_name)
    app_celery.config_from_object('celery_config')


@app_celery.task
def get_image_task(params: dict):
    """This is called by celery worker to retrieve image.
    Parameters
    ----------
    params: `dict`
        the request parameters
    Returns
    -------
    image: `str`
        the file path to the image
    """
    config_path = os.path.join(os.path.dirname(__file__), "../config/")
    config_json = os.path.join(config_path, "imgserv_conf.json")
    with open(config_json) as f:
        config = json.load(f)
        config["DAX_IMG_CONFIG"] = config_path
        config["DAX_IMG_META_URL"] = webserv_config["dax.imgserv.meta.url"]
    soda = imageSODA.ImageSODA(config)
    image = soda.do_sync(params)
    # save the result image in local temp dir
    with tempfile.NamedTemporaryFile(dir=config["DAX_IMG_TEMPDIR"],
                                     prefix="img-",
                                     suffix=".fits",
                                     delete=False) as fp:

        image.writeFits(fp.name)
        # store path to result in Redis
        return fp.name
