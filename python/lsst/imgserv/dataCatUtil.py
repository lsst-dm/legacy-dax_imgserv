

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

class DataCatUtil:
    '''Simple class to work with DataCat
    '''
    def __init__(self, logger=log):
        self._client = Client("http://lsst-db2.slac.stanford.edu:8180/rest-datacat-v1/r")
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
            print "Registering:", path, fileName, fullName
            resp = self._client.path(fullName, versionId="current")
            ds = unpack(resp.content)
            print(ds.__dict__)
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
                        print "Registering:{} as {} c={}".format(fullName, datasetName, c)
                        dsNames.append(datasetName)
                        #resp = self._client.path(fullName, versionId="current")
                        #ds = unpack(resp.content)
                        #print(ds.__dict__)
                    except DcException as e:
                        self._log.warn("create_dataset exception %s %s" % (e, datasetName))
        print "count = ", c

    def directoryCrawlDelete(self, rootDir, prefix):
        '''Crawl throught the directory tree looking for FITS files and
        delete them from dataCat
        '''
        print "directoryCrawlDelete", rootDir, prefix
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
                    self._log.info('\t\t%s is a FITS file %s' %  (fullName, datasetName))
                    fullDsName = self._path + "/" + datasetName
                    print ' deleting {}'.format(fullDsName)
                    try:
                        self._client.delete_dataset(fullDsName)
                    except DcException as e:
                        self._log.warn("delete dataset exception %s %s" % (e, fullDsName))

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
    #if directory.startswith('~'):
    #    directory = os.path.expanduser(directory)
    return os.path.abspath(directory)

def main(argv):
    dcu = DataCatUtil()
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
    print aVals
    client = Client("http://lsst-db2.slac.stanford.edu:8180/rest-datacat-v1/r")
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
            dcu.directoryCrawlRegister(aVals["dirName"], aVals["datasetName"])
        else:
            helpMsg()
    elif cmd == "DEL":
        if aVals["datasetName"] != "":
            dcu.deleteDataset(aVals["datasetName"])
    elif cmd == "DEL_dir":
        print 'DEL_dir {} {}'.format(aVals["dirName"], aVals["datasetName"])
        if aVals["dirName"] != "":
            dcu.directoryCrawlDelete(aVals["dirName"], aVals["datasetName"])
        else:
            helpMsg()
    else:
        helpMsg()
    print "Fini"





if __name__ == '__main__':
    main(sys.argv[1:])
