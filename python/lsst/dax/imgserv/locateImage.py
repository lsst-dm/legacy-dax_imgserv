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
This module is used to locate and retrieve image datasets through the Butler.
(e.g. raw, calexp, deepCoadd), and their related metadata.

"""
from datetime import datetime
import subprocess

import etc.imgserv.imgserv_config as imgserv_config
from lsst.lsst_soda_service.put_values import put_values
from lsst.pipe.tasks.calexpCutout import CalexpCutoutTask
from lsst.daf.butler import Butler
import lsst.afw.image as afw_image

from astropy.coordinates import SkyCoord
from typing import List


def get_data_ids(datasetType: str, positions: List[SkyCoord]) -> List[dict]:
    # This takes a list of positions just in case we want to do many at a time
    # at some point
    data_ids = []
    for position in positions:
        # Get the ids for the dataset type and the position of the cutout
        # This could simply be another service, a call to a database,
        # or a task operating on a repository
        data_ids.append({'visit': 903332, 'detector': 20, 'instrument': 'HSC'})
    return data_ids

def get_image(dagtasetType, shape, *args):
    return getter_map[(datasetType, shape)](*args)

def get_calexp_bbox_image(datasetType: str, ra: float, dec: float, width: float, height: float, unit: str) -> afw_image:
    """
    """
    if datasetType == "default":
        datasetType = imgserv_config.config_datasets["default"]
    if datasetType not in imgserv_config.config_datasets:
        raise ValueError(f'Unsupported dataset type: {datasetType}')
    instrument = imgserv_config.config_datasets[datasetType]['INSTRUMENT']
    repository = imgserv_config.config_datasets[datasetType]['IMG_REPO_ROOT']
    collection = imgserv_config.config_datasets[datasetType]['IMG_DEFAULT_COLLECTION']
    creation_time = datetime.timestamp(datetime.now())
    out_collection = f'imgserv_{creation_time}'
    put_collection = f'imgserv_positions_{creation_time}'
    position = SkyCoord(ra, dec, unit='deg')
    data_id = get_data_ids(datasetType, [position, ])[0]
    visit = data_id['visit']
    detector = data_id['detector']
    size = max([width, height])
    # We will need to generalize this to take arbitrary data IDs
    put_values(repository, visit, detector, instrument,
               put_collection, ra=position.ra.degree, dec=position.dec.degree, size=size)
    # Run the pipeline task: this should block for sync and not for async
    # right now we'll just open a supbrocess
    result = subprocess.run(['pipetask', 'run', '-j', '1', '-b', repository, '--register-dataset-types',
                             '-t', 'lsst.pipe.tasks.calexpCutout.CalexpCutoutTask', '-d',
                             f"visit={visit} AND detector={detector} AND instrument='{instrument}'", '--output', out_collection, '-i',
                             f"{collection},{put_collection}"], capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(f'Non zero return code encountered: {result.returncode}')
    butler = Butler(repository, collections=[out_collection, ])
    stamps = butler.get('calexp_cutouts', dataId=data_id)
    # We know we only sent one position
    return stamps[0].stamp_im

getter_map = {('calexp', 'BBOX'): get_calexp_bbox_image,
             }