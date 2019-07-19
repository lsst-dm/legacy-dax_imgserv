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

import redis
from celery import Celery

from flask import current_app
from ..vo.imageSODA import ImageSODA
from ..hashutil import Hasher


def make_celery(app):
    celery = Celery(app.import_name,
                    backend=app.config["CELERY_RESULT_BACKEND"],
                    broker=app.config["CELERY_BROKER_URL"]
    )
    celery.conf.update(app.config)
    return celery


app_celery = make_celery(current_app)


@app_celery.task
def get_image_task(params: dict, task_id=None):
    """This is called by celery worker to retrieve image.
    Parameters
    ----------
    params: `dict`
        the request parameters
    task_id: `str`
        the unique task id assigned by celery
    Returns
    -------
    image: `lsst.afw.image`
        the image object
    """
    req_key = Hasher.md5(params)
    soda = ImageSODA(current_app.config)
    image = soda.do_async(params)
    # save image in a temp file
    fp = tempfile.NamedTemporaryFile()
    image.writeFits(fp.name)
    # store file in Redis
    r = redis.Redis()
    result = {"task_id": task_id, "req": req_key, "image_result": fp.name}
    r.mset(result)
    return result
