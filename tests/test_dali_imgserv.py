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
"""
This test will launch Imgserv and sodalint (Java lib) process to
test the DALI implementation, without availability of /datasets.
"""
import os
import subprocess

import pytest


@pytest.mark.usefixtures('live_server')
def test_add_endpoint_to_live_server(live_server):
    live_server.start()
    dir_path = os.path.dirname(os.path.abspath(__file__))
    jar_file_path = dir_path + "/sodalint/sodalint-all-1.0.4.jar"
    soda_base_url = live_server.url("/api/image/soda/")
    completed = subprocess.run(["java",
                                "-jar",
                                jar_file_path,
                                soda_base_url], capture_output=True)
    assert(completed.returncode == 0)
    assert("Errors: 0" in str(completed.stdout))
    live_server.stop()

