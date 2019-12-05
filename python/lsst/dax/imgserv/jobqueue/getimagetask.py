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
import os
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase

# use following format for circular reference
import lsst.dax.imgserv.vo.imageSODA as imageSODA

# load config settings for imgserv
import etc.imgserv.imgserv_config as imgserv_config

# for imgserv configuration files (internal)
config_path = os.path.join(os.path.dirname(__file__), "../config/")


class GetImageTaskConfig(pexConfig.Config):
    """!Configuration for ImageTask
    """
    """Configuration for ExampleCmdLineTask.
    """


class GetImageTask(pipeBase.Task):

    ConfigClass = GetImageTaskConfig
    _DefaultName = "GetImageTask"

    def __init__(self, *args, **kwargs):
        pipeBase.Taskinit__(self, *args, **kwargs)

    @pipeBase.timeMethod
    def runDataRef(self, dataRef):
        config = imgserv_config.config_datasets["default"]
        config["DAX_IMG_CONFIG"] = config_path
        meta_url = imgserv_config.webserv_config["dax.imgserv.meta.url"]
        config["DAX_IMG_META_URL"] = meta_url
        soda = imageSODA.ImageSODA(config)
        cutout = soda.do_sync(dataRef)
        return pipeBase.Struct(image=cutout)


