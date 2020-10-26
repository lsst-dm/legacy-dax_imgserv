"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documentation builds.
"""

from documenteer.sphinxconfig.stackconf import build_package_configs
import lsst.dax.imgserv

from lsst.dax.imgserv import version
imgserv_version = version.__version__

_g = globals()
_g.update(build_package_configs(
    project_name='imgserv',
    version=imgserv_version))
