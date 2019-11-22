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
import traceback
from datetime import datetime

from celery import Celery, states
from celery.exceptions import Ignore

from flask import current_app

# use following format for circular reference
import lsst.dax.imgserv.vo.imageSODA as imageSODA

# load config settings for imgserv
import etc.imgserv.imgserv_config as imgserv_config

# Load and configure the celery app
app_celery = Celery("image_worker")
app_celery.config_from_object('etc.celery.celery_config')

# for imgserv configuration files (internal)
config_path = os.path.join(os.path.dirname(__file__), "../config/")

# for keeping track of info per task_id
g_task_info = dict({})


def make_celery():
    """
        Initiate the celery app for client access.
    """
    global app_celery
    app_celery = Celery(current_app.import_name)
    app_celery.config_from_object('celery_config')


# noinspection PyBroadException
@app_celery.task(bind=True)
def get_image_async(self, *args, **kwargs):
    """This is called by celery worker to retrieve image.

    Parameters
    ----------
    self: `app.Task`
    params: `dict`
        the request parameters

    Returns
    -------
    result: `dict`
        dax_result: `str`
            the file path to the image output.
        dax_start_time: `int`
            the job start time.
        dax_end_time: `int`
            the job completion time.
    """
    job_start_time = datetime.timestamp(datetime.now())
    params = args[0]
    print("get_image_async called with request params="+str(params))
    job_creation_time = kwargs.get("job_creation_time")
    job_owner = kwargs.get("owner")
    config = imgserv_config.config_json
    config["DAX_IMG_CONFIG"] = config_path
    meta_url = imgserv_config.webserv_config["dax.imgserv.meta.url"]
    config["DAX_IMG_META_URL"] = meta_url
    soda = imageSODA.ImageSODA(config)
    image = soda.do_sync(params)
    # save the result image in local temp dir
    with tempfile.NamedTemporaryFile(dir=config["DAX_IMG_TEMPDIR"],
                                     prefix="img-",
                                     suffix=".fits",
                                     delete=False) as fp:
        image.writeFits(fp.name)
    job_end_time = datetime.timestamp(datetime.now())
    result = {
        "job_result": fp.name,
        "job_owner": job_owner,
        "job_creation_time": job_creation_time,
        "job_start_time": job_start_time,
        "job_end_time": job_end_time,
        "soda_params": params
    }
    return result
