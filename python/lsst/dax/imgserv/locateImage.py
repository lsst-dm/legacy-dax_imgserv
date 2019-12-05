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

import lsst.afw.image as afw_image

from .getimage.imageget import ImageGetter
from .butlerGet import ButlerGet
from .metaGet import MetaGet
from .dispatch import Dispatcher
from .exceptions import UsageError


def open_image(ds, ds_type, config) -> ImageGetter:
    """Open access to specified images (raw, calexp, deepCoadd,etc) of
    specified image repository.

    Parameters
    ----------

    ds: `str`
        the dataset identifier.
    ds_type : `str`
        the dataset type.
    config: `dict`
        the application configuration.

    Returns
    -------
    imagegetter : `ImageGetter`
        instance for access to all image operations.

    """
    repo_root = config["IMG_REPO_ROOT"]
    dataid_keys = config.get(ds_type, None)
    if dataid_keys is None:
        raise UsageError("Unrecognized image dataset type")
    butler_get = ButlerGet.get_butler(ds, repo_root, ds_type, dataid_keys)
    meta_get = MetaGet(config)
    return ImageGetter(config, butler_get, meta_get)


def get_image(params: dict, config: dict) -> afw_image:
    """Get the image per query request synchronously (default).

    Parameters
    ----------
    params : `dict`
        the request parameters.
    config : `dict`
        the config file.

    Returns
    -------
    image : afw_image
        the requested image if found.

    """
    dispatcher = Dispatcher(config["DAX_IMG_CONFIG"])
    api, api_params = dispatcher.find_api(params)
    ds = api_params.get("ds", None)
    if ds is None:
        raise UsageError("Missing dataset identifier")
    ds_type = api_params.get("dsType", None)
    if ds_type is None:
        raise UsageError("Missing dsType parameter")
    img_getter = open_image(ds, ds_type, config)
    image = api(img_getter, api_params)
    return image