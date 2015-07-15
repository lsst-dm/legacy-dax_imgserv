#
# LSST Data Management System
# Copyright 2014-2015 LSST/AURA.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import ConfigParser
import getopt
import sys
import os
import sched, time
import subprocess
from datacat import Client, unpack
from datacat.config import CONFIG_URL
from datacat.client import DcException

from datetime import datetime

from lsst.imgserv.MetadataFitsDb import isFits
import lsst.log as log

class DataCatCfg():
    '''Class to load and stor configuration information for DataCat
    '''
    def __init__(self, cfgFile="~/.datacat.cfg", logger=log):
        '''Set default values if cfgFile is not found.
        '''
        self._log = logger
        if cfgFile.startswith('~'):
            cfgFile = os.path.expanduser(cfgFile)
        self._cfgFile = expandDir(cfgFile)
        self._config = ConfigParser.ConfigParser()
        self._restUrl = ("http://lsst-db2.slac.stanford.edu:8180/"
                         "rest-datacat-v1/r")
        self.read()

    def read(self):
        self._config.read(self._cfgFile)
        try:
            self._restUrl = self._config.get("DataCat", "restUrl")
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError) as exc:
            self._log.warn("DataCatCfg {}".format(exc))

    def getRestUrl(self):
        return self._restUrl

class DataCatUtil:
    '''Simple class to work with DataCat
    '''
    def __init__(self, dataCatCfg, logger=log):
        self._dataCatCfg = dataCatCfg
        self._client = Client(self._dataCatCfg.getRestUrl())
        self._log = logger
        self._site = "SLAC"
        self._path = "/LSST"
        self._dataType = "FITSIMAGE"
        self._fileFormat = "fits"
        self._versionId = "new"
        self._slashSub = "."

    def registerFile(self, fileName, datasetName):
        self._log.info("registerFile %s %s" % (fileName, datasetName))
        fileName = expandDir(fileName)
        try:
            path=self._path
            self._client.create_dataset(path=path, name=datasetName,
                                        dataType=self._dataType, fileFormat=self._fileFormat,
                                        versionId=self._versionId, site=self._site,
                                        resource=fileName)
            fullName = path+"/"+fileName
            self._log.info("Registering:", path, fileName, fullName)
            resp = self._client.path(fullName, versionId="current")
            ds = unpack(resp.content)
            self._log.info(ds.__dict__)
        except DcException as e:
            self._log.warn("create_dataset exception %s %s" % (e, datasetName))

    def deleteDataset(self, dsName):
        fullDsName = self._path + "/" + dsName
        self._log.info("deleteDataset %s" % fullDsName)
        try:
            self._client.delete_dataset(fullDsName)
        except DcException as e:
            self._log.warn("DcException, probably not deleted %s %s" % (e, fullDsName))

    def directoryCrawlRegister(self, rootDir, prefix):
        '''Crawl throught the directory tree looking for FITS files and
        register them with DataCat
        '''
        c = 0
        dsNames = []
        rootDir = expandDir(rootDir)
        for dirName, subdirList, fileList in os.walk(rootDir):
            self._log.info('Found directory: %s' % dirName)
            for fName in fileList:
                fullName = os.path.join(dirName, fName)
                self._log.info(' %s'%  fullName)
                if isFits(fullName):
                    parts = fullName.split('/')
                    datasetName = prefix  + self._slashSub.join(parts)
                    self._log.info('  %s is a FITS file %s' %  (fullName, datasetName))
                    try:
                        self._client.create_dataset(path=self._path, name=datasetName,
                                                    dataType=self._dataType,
                                                    fileFormat=self._fileFormat,
                                                    versionId=self._versionId, site=self._site,
                                                    resource=fullName)
                        c += 1
                        self._log.info("Registering:%s as %s", fullName, datasetName)
                        dsNames.append(datasetName)
                        resp = self._client.path(fullName, versionId="current")
                        ds = unpack(resp.content)
                        self._log.info("response %s", ds.__dict__)
                    except DcException as e:
                        self._log.warn("create_dataset exception %s %s", e, datasetName)
        return c

    def directoryCrawlDelete(self, rootDir, prefix):
        '''Crawl throught the directory tree looking for FITS files and
        delete them from dataCat
        '''
        self._log.info("directoryCrawlDelete %s %s", rootDir, prefix)
        c = 0
        dsNames = []
        for dirName, subdirList, fileList in os.walk(rootDir):
            self._log.info('Found directory: %s' % dirName)
            for fName in fileList:
                fullName = os.path.join(dirName, fName)
                self._log.info('\t%s'%  fullName)
                if isFits(fullName):
                    parts = fullName.split('/')
                    datasetName = prefix + self._slashSub.join(parts)
                    fullDsName = self._path + "/" + datasetName
                    self._log.info(' deleting {}'.format(fullDsName))
                    try:
                        self._client.delete_dataset(fullDsName)
                        c += 1
                    except DcException as e:
                        self._log.warn("delete dataset exception %s %s", e,
                                       str(fullDsName))
        return c




def helpMsg():
    print ' --DEL --ds <dataset>                  // delete the dataset from DataCat'
    print ' --regf <fileName> --ds <datasetName> // register file'
    print ' --regd <directory> --ds <datasetPrefix> // recursively register files in directory'
    print '                                  // with a datasetName prefix'
    print ' --DEL_dir <directory> --ds <datasetPrefix> // recursively remove files in directory'
    print '                                  // from DataCat with datasetPrefix'
    print '                                  // This does not delete the files.'


def expandDir(directory):
    '''Make the directory an absolute path'''
    return os.path.abspath(directory)

def main(argv):
    dataCatCfg = DataCatCfg()
    dcu = DataCatUtil(dataCatCfg)
    aVals = { "fileName":"", "dirName":"", "datasetName":"", "command":"" }
    try:
        opts, args = getopt.getopt(argv,"h",["DEL","regf=","regd=","ds=","DEL_dir="])
    except getopt.GetoptError:
        helpMsg()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            helpMsg()
            sys.exit()
        elif opt in ("--regf"):
            aVals["fileName"] = expandDir(arg)
            aVals["command"] = "regf"
        elif opt in ("--regd"):
            aVals["dirName"] = expandDir(arg)
            aVals["command"] = "regd"
        elif opt in ("--DEL"):
            if aVals["command"] == "":
                aVals["command"] = "DEL"
        elif opt in ("--DEL_dir"):
            if aVals["command"] == "":
                aVals["command"] = "DEL_dir"
            aVals["dirName"] = expandDir(arg)
        elif opt in ("--ds"):
            aVals["datasetName"] = arg
    cmd = aVals["command"]
    if cmd == "regf":
        print 'regf {} {}'.format(aVals["fileName"], aVals["datasetName"])
        if aVals["fileName"] != "" and aVals["datasetName"] != "":
            dcu.registerFile(aVals["fileName"], aVals["datasetName"])
        else:
            helpMsg()
    elif cmd == "regd":
        print 'regd {}'.format(aVals["dirName"])
        if aVals["dirName"] != "":
            count = dcu.directoryCrawlRegister(aVals["dirName"], aVals["datasetName"])
            print "Registered {} files".format(count)
        else:
            helpMsg()
    elif cmd == "DEL":
        if aVals["datasetName"] != "":
            dcu.deleteDataset(aVals["datasetName"])
    elif cmd == "DEL_dir":
        print 'DEL_dir {} {}'.format(aVals["dirName"], aVals["datasetName"])
        if aVals["dirName"] != "":
            count = dcu.directoryCrawlDelete(aVals["dirName"], aVals["datasetName"])
            print "Deleted from DataCat {} files".format(count)
        else:
            helpMsg()
    else:
        helpMsg()
    print "Done"

if __name__ == '__main__':
    main(sys.argv[1:])
