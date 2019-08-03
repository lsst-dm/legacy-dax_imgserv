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
import datetime

from celery import Celery
from flask import current_app

# use following format for circular reference
import lsst.dax.imgserv.vo.imageSODA as imageSODA

# load config settings for imgserv
import etc.imgserv.imgserv_config as imgserv_config

# Load and configure the celery app
app_celery = Celery("Queue Worker")
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
    app = current_app
    app_celery = Celery(app.import_name)
    app_celery.config_from_object('celery_config')


@app_celery.task(bind=True)
def get_image_async(self, params: dict):
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
        dax_start_time: `str`
            the job start time.
        dax_end_time: `str`
            the job finish time.
        dax_duration: `str`
            how long it took.
    """
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
        # store path to result in Redis
        job_start_time = g_task_info[self.request.id+".start_time"]
        job_end_time = g_task_info[self.request.id+".succeeded_time"]
        job_duration = job_end_time - job_start_time
        result = {
            "job_result": fp.name,
            "job_start_time": str(job_start_time),
            "job_end_time": str(job_end_time),
            "job_duration": str(job_duration)
        }
        return result


def imageworker_monitor(app):
    state = app.events.State()

    def on_task_failed(event):
        state.event(event)
        task = state.tasks.get(event['uuid'])
        print('TASK FAILED: %s[%s] %s' % (task.name, task.uuid, task.info(),))

    def on_task_succeeded(event):
        state.event(event)
        task = state.tasks.get(event['uuid'])
        g_task_info[task.uuid+".succeeded_time"]=datetime.datetime.now()
        print('TASK SUCCEEDED: %s[%s] %s' % (task.name,
                                             task.uuid, task.info(),))

    def on_task_sent(event):
        state.event(event)
        task = state.tasks.get(event['uuid'])
        print('TASK SENT: %s[%s] %s' % (task.name, task.uuid, task.info(),))

    def on_task_received(event):
        state.event(event)
        # task name is sent only with -received event, and state
        # will keep track of this for us.
        task = state.tasks.get(event['uuid'])
        print('TASK RECEIVED: %s[%s] %s' % (task.name, task.uuid, task.info(),))

    def on_task_revoked(event):
        state.event(event)
        task = state.tasks.get(event['uuid'])
        print('TASK REVOKED: %s[%s] %s' % (task.name, task.uuid, task.info(),))

    def on_task_started(event):
        state.event(event)
        task = state.tasks.get(event['uuid'])
        g_task_info[task.uuid+".start_time"]=datetime.datetime.now()
        print('TASK STARTED: %s[%s] %s' % (task.name, task.uuid, task.info(),))

    with app.connection() as connection:
        recv = app.events.Receiver(connection, handlers={
            'task-failed': on_task_failed,
            'task-succeeded': on_task_succeeded,
            'task-sent': on_task_sent,
            'task-received': on_task_received,
            'task-revoked': on_task_revoked,
            'task-started': on_task_started,
            '*': state.event,
        })
        recv.capture(limit=None, timeout=None, wakeup=True)


if __name__ == '__main__':
    imageworker_monitor(app_celery)
