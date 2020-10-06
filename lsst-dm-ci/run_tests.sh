#!/bin/sh

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

# Launched by LSST Jenkins-CI

# stop execution on error
set -e

# setup lsst stack
source /opt/lsst/software/stack/loadLSST.bash
setup lsst_distrib

# install Java JRE for sodalint tests
if [ -f /usr/bin/java ]
then
  printf "Java already installed."
else
  yum -y install java-11-openjdk
fi

# setup imgserv
cd /app
pip install --no-cache-dir --user -r requirements.txt
pip install --no-cache-dir --user .

#run pytest
cd /app/tests
pytest
printf "Run Image %s tests successfully\n" "$TAG"
