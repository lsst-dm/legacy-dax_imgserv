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

set -e

# Build and push container image for ImgServ production version
# FIXME: $rep check due to wrong value from jenkins-dm-jobs
if [[ -z "$DOCKER_REPO" || "$DOCKER_REPO" == '$repo' ]]; then
    DOCKER_REPO="webserv/imgserv"
fi

TAG="$DOCKER_REPO:dax_latest"
DOCKERDIR="../"

# Build the image
printf "Building image %s from %s\n" "$TAG" "$DOCKERDIR"
docker build --tag="$TAG" "$DOCKERDIR"

printf "Image %s built successfully\n" "$TAG"
