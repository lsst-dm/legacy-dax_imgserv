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

# List of modules to import
imports = ('lsst.dax.imgserv.jobqueue',)

# Use redis for message broker
broker_url = "redis://localhost:6379"

# Use redis for storing the results
result_backend = "redis://localhost:6379/0"

# Use imageworker queue
task_routes = {'feed.tasks.import_feed': {'queue': 'imageworker_queue'}}

# worker event setting
worker_send_task_events = True

# task event setting
task_send_sent_event = True

# Enable STARTED state
task_track_started = True

task_annotations = {"jobqueue.imageworker": {"description": "ImgServ Worker"}}
