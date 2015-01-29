#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2014 LSST Corporation.
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

# The purpose of this is to read all the FITS files in a directory tree
# and insert their header information into the metadata database.


import gzip
import MySQLdb
import os
import sys
import time


import lsst.afw
import lsst.afw.image as afwImage
import lsst.daf.base as dafBase
import lsst.log as log

from lsst.cat.dbSetup import DbSetup
from lsst.db.utils import readCredentialFile



def isDateFormatValid(dt):
    try:
        time.strptime(dt, "%Y-%m-%d %H:%M:%S")
        return True
    except ValueError:
        return False

def executeInsertList(cursor, table, columnValues, logger=log):
    '''Insert the columnValues into 'table'
    columnValue is a list of column name and value pairs.
    Values are sanitized.
    '''
    if len(columnValues) < 1:
        return
    sql_1 = "INSERT INTO {} (".format(table)
    colStr = ""
    valStr = ""
    values = []
    first = True
    for col, val in columnValues:
        values.append(val)
        if first:
            first = False
            colStr = "{}".format(col)
            valStr = "%s"
        else:
            colStr += ", {}".format(col)
            valStr += ", %s"
    sql = sql_1 + colStr + ") Values (" + valStr + ")"
    # '%s' in sql causes lsst.log to have problems, so we log its component pieces.
    logger.debug("InsertList %s %s) VALUES (%s)", sql_1, colStr, values)
    cursor.execute(sql, values)


class ExpectedHduError(Exception):
    def __init__(self):
        self.value = "Next HDU not found, which is expected but could be hiding a more serious C error."

    def __str__(self):
        return repr(self.value)


class MetadataFits:
    '''Class for reading FITS headers and temporary storage of header values'''
    def __init__(self, fileName, logger=log):
        self._fileName = fileName
        self._hdus = 1
        self._entries = {}
        self._log = logger

    def scanFileAllHdus(self):
        hdu = 1
        while True:
            try:
                self._log.info("Scanning %s %d", str(self._fileName), hdu)
                self.scanFile(hdu)
                self._hdus = hdu
                hdu += 1
            except  ExpectedHduError as err:
                self._log.info("exception while scanning %d %s", hdu, err)
                break

    def getFileName(self):
        return self._fileName

    def getHdus(self):
        return self._hdus

    def scanFile(self, hdu):
        # The only way to tell that the last HDU has been read is there is a C++ error.
        # We catch all exceptions and assume it's the 'past end of file' error.
        try:
            meta = afwImage.readMetadata(self._fileName, hdu)
        except:
            raise ExpectedHduError
        # Cast to PropertyList to access more extensive information
        metaPl = dafBase.PropertyList.cast(meta)
        names = metaPl.getOrderedNames()
        lineNum = 0
        for name in names:
             data = metaPl.get(name)
             comment = metaPl.getComment(name)
             self._entries[(name,hdu)] = (data, lineNum, comment)
             # Increment the line number by 1 for each value so they can be expanded later
             # in insertMetadataFits.
             if isinstance(data, tuple):
                 lineNum += len(data)
             else:
                 lineNum += 1
        #try:
        #    wcs = afwImage.makeWcs(metaPl)
        #except:
        #    pass

    def dump(self):
        s = "File:{} HDUs:{}\n".format(self._fileName, self._hdus)
        for key, value in self._entries.iteritems():
            s += "( {}:{} ) = ({}, {}, {})\n".format(key[0], key[1], value[0], value[1], value[2])
        return s


class MetadataPosition:
    '''Insert the position information for the FITS file/hdu into the database.
    '''
    def __init__(self, fileId, hdu, cursor, entries, logger=log):
        self._fileId = fileId
        self._hdu = hdu
        self._cursor = cursor
        self._entries = entries
        self._columnKeys = (("equinox","EQUINOX"), ("pRa","PRA"), ("pDec","PDEC"), 
                           ("rotAng","ROTANG"), ("pDate","DATE"))
        self._log = logger

    def _insert(self):
        # Figure out the date
        columns = {}
        if ('DATE', self._hdu) in self._entries:
            dt = self._entries[('DATE', self._hdu)][0]
            dt = dt.replace('T',' ')
            if isDateFormatValid(dt):
                columns['pdate'] = dt
        found = False
        # Set equinox, use EQUINOX if it exists, otherwise use EPOC (deprecated)
        if ('EQUINOX', self._hdu) in self._entries:
            eq = self._entries[('EQUINOX', self._hdu)][0]
            try:
                columns['equinox'] = float(eq)
                found = True
            except ValueError:
                pass
        if not found:
            if ('EPOC', self._hdu) in self._entries:
                eq = self._entries[('EPOC', self._hdu)][0]
                try:
                    columns['equinox'] = float(eq)
                    found = True
                except ValueError:
                    pass
        for colKey in self._columnKeys:
            column = colKey[0]
            if column != 'pdate' and column != 'equinox':
                key = colKey[1]
                if (key, self._hdu) in self._entries:
                    value = self._entries[(key, self._hdu)][0]
                    try:
                        columns[column] = float(value)
                    except:
                        pass
        # if any column values were successfully defined, insert the row into the table
        if len(columns) > 0:
            colVal = [("fitsFileId", self._fileId), ("hdu", self._hdu)]
            for col, val in columns.iteritems():
                colVal.append((col, val))
            executeInsertList(self._cursor, "FitsPositions", colVal, self._log)




class MetadataFitsDb:
    '''This class is used to collect Metadata from FITS header information
       and place it in the database.
    '''
    def __init__(self, dbHost, dbPort, dbUser, dbPasswd, dbName, logger=log):
        self._log = logger
        self._connect = MySQLdb.connect(host=dbHost,
                      port=dbPort,
                      user=dbUser,
                      passwd=dbPasswd,
                      db=dbName)
        cursor = self._connect.cursor()
        sql = "SET time_zone = '+0:00'"
        try:
            self._log.info(sql)
            cursor.execute(sql)
        except MySQLdb.Error as err:
            self._log.info("ERROR MySQL %s", err)
        cursor.close()

    def close(self):
        self._connect.close()

    def showTables(self):
        cursor = self._connect.cursor()
        cursor.execute("SHOW TABLES")
        ret = cursor.fetchall()
        s = str(ret)
        for tbl in ret:
            cursor.execute("SHOW COLUMNS from %s" % (tbl[0]))
            s += str(cursor.fetchall())
        cursor.close()
        return s

    def _insertFitsValue(self, cursor, fitsFileId, key, value, lineNum, comment):
        '''Insert a Fits row entry into the FitsKeyValues table
           for all keywords found in the table.
           The calling function is expected to handle exceptions.
        '''
        colVal = [("fitsFileId", fitsFileId), ("fitsKey", key[0]), ("hdu", key[1]),
                  ("stringValue", value), ("lineNum", lineNum), ("comment", comment) ]
        intValue = 'NULL'
        doubleValue = 'NULL'
        try:
            # Converting floats to int results in very inaccurate
            # integer values in some cases, so it is avoided.
            if not isinstance(value, float):
                intValue = int(value)
                colVal.append(("intValue", intValue))
        except ValueError:
            pass
        try:
            doubleValue = float(value)
            colVal.append(("doubleValue", doubleValue))
        except ValueError:
            pass
        executeInsertList(cursor, "FitsKeyValues", colVal, self._log)

    def insertFile(self, fileName):
        '''Insert the header information for 'fileName' into the
        MetadataDatabase metaDb, but only if it is a FITS file.
        It returns the FitsFiles.fitsFileId of the file added, or -1 if nothing
        was added.
        '''
        returnVal = -1
        if isFits(fileName):
            mdFits = MetadataFits(fileName)
            mdFits.scanFileAllHdus()
            returnVal = self.insertMetadataFits(mdFits)
        return returnVal

    def isFileInDb(self, fileName):
        '''Test if this filename in the database
        '''
        cursor = self._connect.cursor()
        sql = ("SELECT 1 FROM FitsFiles WHERE "
               "fileName = %s")
        self._log.debug(sql, fileName)
        cursor.execute(sql, fileName)
        r = cursor.fetchall()
        return len(r) >= 1


    def insertMetadataFits(self, metadata):
        '''Insert this FITS file's and its key:value pairs into the database.
        It returns the FitsFiles.fitsFileId of the file added, or -1 if nothing
        was added.
        '''
        fileName = metadata.getFileName()
        hdus     = metadata.getHdus()
        entries  = metadata._entries
        lastFitsFileId = -1
        # Check if the file is in the database, and if not add it
        cursor = self._connect.cursor()
        try:
            sql = ("SELECT 1 FROM FitsFiles WHERE "
                   "fileName = %s")
            self._log.debug(sql, str(fileName))
            cursor.execute(sql, fileName)
            r = cursor.fetchall()
            # Nothing found, so it needs to be added.
            if len(r) < 1:
                #If this fails for any reason, we do not want the database altered.
                cursor.execute("START TRANSACTION")
                cursor.execute("SET autocommit = 0")
                # Insert the file into the file table.
                colVal = [("fileName", fileName), ("hduCount", hdus)]
                executeInsertList(cursor, "FitsFiles", colVal, self._log)
                lastFitsFileId = cursor.lastrowid
                for key, entry in entries.iteritems():
                    value   = entry[0]
                    lineNum = entry[1]
                    comment = entry[2]
                    # Put in one entry for each element of the tuple
                    if isinstance(value, tuple):
                        num = lineNum
                        for v in value:
                            # scanFile should be skipping line numbers when it sees a tuple,
                            # so that there are no duplicates.
                            self._insertFitsValue(cursor, lastFitsFileId, key, v, num, comment)
                            num += 1
                    else:
                        self._insertFitsValue(cursor, lastFitsFileId, key, value, lineNum, comment)
                for hdu in range(1,hdus+1):
                    metadataPosition = MetadataPosition(lastFitsFileId, hdu, cursor, entries)
                    metadataPosition._insert()
                cursor.execute("COMMIT")
        except MySQLdb.Error as err:
            cursor.execute("ROLLBACK")
            self._log.warn( "ROLLBACK due to ERROR MySQLdb %s --%s", err, sql)
            quit() # TODO delete this line, for now it is good to stop and examine these
        cursor.close()
        return lastFitsFileId

def dbOpen(credFileName, dbName, portDb=3306, logger=log):
    creds = readCredentialFile(credFileName, logger)
    port = portDb
    if 'port' in creds:
        port = int(creds['port'])
    mdFits = MetadataFitsDb(dbHost=creds['host'], dbPort=port,
                            dbUser=creds['user'], dbPasswd=creds['passwd'],
                            dbName=dbName)
    return mdFits

def dbDestroyCreate(credFileName, userDb, code, logger=log):
    '''Open the database userDb, delete tables, then re-create them.
    '''
    creds = readCredentialFile(credFileName, logger)
    port = 3306
    if 'port' in creds:
        port = int(creds['port'])
    if (code == "DELETE"):
        logger.info("DbSetup attempting to delete and then create %s", userDb)
        db = DbSetup(creds['host'], port, creds['user'], creds['passwd'],
                     dirEnviron="IMGSERV_DIR", subDir="sql", userDb=userDb)
        scripts = ["fitsMetadataSchema.sql"]
        db.setupDb(scripts)
        logger.info("DbSetup done")
    else:
        logger.warn("code not supplied, database un-altered. %s", userDb)

def isFitsExt(fileName):
    '''Return True if the file extension is reasonable for a FITS file.
    '''
    nameSplit = fileName.split('.')
    length = len(nameSplit)
    if length < 2:
        return False
    last = nameSplit[-1]
    if last == 'fits':
        return True
    if last == 'gz' and nameSplit[-2] == 'fits':
        return True
    return False

def isFits(fileName):
    '''Return True if a quick peeks says that this is a FITS file.
    '''
    if not isFitsExt(fileName):
        return False
    if fileName.split('.')[-1] == 'gz':
        f = gzip.open(fileName)
    else:
        f = open(fileName, 'r')
    line = f.read(9)
    if line == 'SIMPLE  =':
        return True

def directoryCrawl(rootDir, metaDb):
    '''Crawl throught the directory tree looking for FITS files.
       Parse the FITS headers for each FITS file found and put them in the database.
    '''
    logger = metaDb._log
    for dirName, subdirList, fileList in os.walk(rootDir):
        logger.info('Found directory: %s', dirName)
        for fname in fileList:
            fullName = dirName+'/'+fname
            logger.info('\t%s', fullName)
            metaDb.insertFile(fullName)


def test(rootDir="~/test_metadata"):
    '''This test only works on specific servers and uses a large dataset.
    '''
    credFile = "~/.mysqlAuthLSST"
    creds = readCredentialFile(credFile, log)
    dbName = "{}_fitsTest".format(creds['user'])

    # Destroy existing tables and re-create them
    dbDestroyCreate(credFile, dbName, "DELETE")

    # Open a connection to the database.
    metadataFits = dbOpen(credFile, dbName)

    #root = '/lsst3/DC3/data/obs/ImSim/pt1_2/eimage/v886946741-fi/E000'
    log.debug(rootDir)
    if rootDir.startswith('~'):
        rootDir = os.path.expanduser(rootDir)
    log.debug(rootDir)
    directoryCrawl(rootDir, metadataFits)
    metadataFits.close()

def deleteTestDb():
    credFile = "~/.mysqlAuthLSST"
    creds = readCredentialFile(credFile, log)
    dbName = "{}_fitsTest".format(creds['user'])
    # Destroy existing tables and re-create them
    dbDestroyCreate(credFile, dbName, "DELETE")

if __name__ == "__main__":
    log.setLevel("", log.DEBUG)
    if len(sys.argv) > 1:
        if (sys.argv[1] == "-DELETE"):
            deleteTestDb()
        else:
            test(sys.argv[1])
    else:
        test()




