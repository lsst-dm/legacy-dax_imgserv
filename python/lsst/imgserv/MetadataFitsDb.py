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



import os
import time
import logging

import gzip
import MySQLdb

import lsst.afw
import lsst.afw.image as afwImage
import lsst.daf.base as dafBase

from lsst.cat.dbSetup import DbSetup
from lsst.db.utils import readCredentialFile



def isDateFormatValid(dt):
    try:
        time.strptime(dt, "%Y-%m-%d %H:%M:%S")
        return True
    except ValueError:
        return False


class ExpectedHduError(Exception):
    def __init__(self):
        self.value = "Next HDU not found. This could be hiding a more serious C error."

    def __str__(self):
        return repr(self.value)


class MetadataFits:
    '''Class for reading FITS headers and temporary storage of header values'''
    def __init__(self, fileName):
        self._fileName = fileName
        self._hdus = 1
        self._entries = {}

    def scanFileAllHdus(self):
        hdu = 1
        while True:
            try:
                print "Scanning ", self._fileName, hdu
                self.scanFile(hdu)
                self._hdus = hdu
                hdu += 1
            except  ExpectedHduError as err:
                print "exception while scanning ", hdu, err
                break

    def getFileName(self):
        return self._fileName

    def getHdus(self):
        return self._hdus

    def scanFile(self, hdu):
        # The only way to tell that the last HDU has been read is there is a C++ error.
        # We catch all exceptions and assume it's the past end of file error and
        # raise a specific exception. Catching everything later was horrifyingly good at
        # hiding problems.
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

    def dump(self):
        s = "File:{} HDUs:{}\n".format(self._fileName, self._hdus)
        for key, value in self._entries.iteritems():
            s += "( {}:{} ) = ({}, {}, {})\n".format(key[0], key[1], value[0], value[1], value[2])
        return s


class MetadataPosition:
    '''Insert the position information for the FITS file/hdu into the database.'''
    def __init__(self, fileId, hdu, cursor, entries):
        self._fileId = fileId
        self._hdu = hdu
        self._cursor = cursor
        self._entries = entries
        self._columnKeys = (("equinox","EQUINOX"), ("pra","PRA"), ("pdec","PDEC"), 
                           ("rotang","ROTANG"), ("pdate","DATE"))

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
            sql_1 = "INSERT INTO FitsPositions (fitsFileId, hdu"
            sql_2 = ") VALUES ({}, {}".format(self._fileId, self._hdu)
            for col, val in columns.iteritems():
                sql_1 += ", {}".format(col)
                sql_2 += ", " + valueSql(val)
            sql = sql_1 + sql_2 + ")"
            self._cursor.execute(sql)


def valueSql(value):
    '''Return a string encapsulated by single quotes for strings, otherwise just a returns
       a string version of the value. This makes SQL happier.'''
    if isinstance(value,  str):
        return "'{}'".format(value)
    return "{}".format(value)


class MetadataFitsDb:
    '''This class is used to collect Metadata from FITS header information
       and place it in the database.
    '''
    def __init__(self, dbHost, dbPort, dbUser, dbPasswd, dbName):
        self._connect = MySQLdb.connect(host=dbHost,
                      port=dbPort,
                      user=dbUser,
                      passwd=dbPasswd,
                      db=dbName)
        cursor = self._connect.cursor()
        sql = "SET time_zone = '+0:00'"
        try:
            cursor.execute(sql)
        except MySQLdb.Error as err:
            print "ERROR MySQL {} --  {}".format(err, crt)
        cursor.close()

    def close(self):
        self._connect.close()

    def showTables(self):
        cursor = self._connect.cursor()
        cursor.execute("SHOW TABLES")
        ret = cursor.fetchall()
        print ret
        for tbl in ret:
            cursor.execute("SHOW COLUMNS from {}".format(tbl[0]))
            print cursor.fetchall()
        cursor.close()

    def _insertFitsValue(self, cursor, fitsFileId, key, value, lineNum, comment):
        '''Insert a Fits row entry into the FitsKeyValues table
           for all keywords found in the table.
           The calling function is expected to handle exceptions.
        '''
        intValue = 'NULL'
        doubleValue = 'NULL'
        try:
            # Converting floats to int results in very inaccurate
            # integer values in some cases, so it is avoided.
            if not isinstance(value, float):
                intValue = int(value)
        except ValueError:
            pass
        try:
            doubleValue = float(value)
        except ValueError:
            pass
        sql = ("INSERT INTO FitsKeyValues "
               "(fitsFileId, fitsKey, hdu, stringValue, intValue, doubleValue, lineNum, comment) "
               "VALUES ({}, '{}', {}, '{}', {}, {}, {}, '{}')".format(
                fitsFileId, key[0], key[1], value, intValue, doubleValue, lineNum, comment))
        cursor.execute(sql)

    def insertFile(self, fileName):
        '''Insert the header information for 'fileName' into the
        MetadataDatabase metaDb, but only if it is a FITS file.
        '''
        if isFits(fileName):
            mdFits = MetadataFits(fileName)
            mdFits.scanFileAllHdus()
            self.insertMetadataFits(mdFits)
            # print mdFits.dump()

    def insertMetadataFits(self, metadata):
        '''Insert this FITS file's and its key:value pairs into the database.
        '''
        fileName = metadata.getFileName()
        hdus     = metadata.getHdus()
        entries  = metadata._entries
        # Check if the file is in the database, and if not add it
        cursor = self._connect.cursor()
        try:
            sql = ("SELECT 1 FROM FitsFiles WHERE "
                   "fileName = '{}'".format(fileName))
            cursor.execute(sql)
            r = cursor.fetchall()
            # Nothing found, so it needs to be added.
            if len(r) < 1:
                #If this fails for any reason, we do not want the database altered.
                cursor.execute("START TRANSACTION")
                cursor.execute("SET autocommit = 0")
                sql = ("INSERT INTO FitsFiles (fileName, hduCount) "
                       "VALUES ('{}', {})".format(fileName, hdus))
                cursor.execute(sql)
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
            print "ROLLBACK due to ERROR MySQLdb {} -- {}".format(err, sql)
            quit() # TODO delete this line, for now it is good to stop and examine these.
        cursor.close()

def dbOpen(credFileName, dbName):
    creds = readCredentialFile(credFileName, logging.getLogger("lsst.imgserv.metadatafits"))
    port = int(creds['port'])
    mdFits = MetadataFitsDb(dbHost=creds['host'], dbPort=port,
                            dbUser=creds['user'], dbPasswd=creds['passwd'],
                            dbName=dbName)
    return mdFits

def dbTestDestroyCreate(credFileName, userDb, code):
    '''Open the test database, delete tables, then re-create them.
    '''
    creds = readCredentialFile(credFileName, logging.getLogger("lsst.imgserv.metadatafits"))
    port = int(creds['port'])
    if (code == "DELETE"):
        print "DbSetup attempting to delete and then create", userDb
        db = DbSetup(creds['host'], port, creds['user'], creds['passwd'],
                     dirEnviron="IMGSERV_DIR", subDir="sql", userDb=userDb)
        scripts = ["fitsMetadataSchema.sql"]
        db.setupDb(scripts)
    else:
        print "code not supplied, database un-altered.", userDb

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
    s = line
    if s == 'SIMPLE  =':
        return True

def directoryCrawl(rootDir, metaDb):
    '''Crawl throught the directory tree looking for FITS files.
       Parse the FITS headers for each FITS file found and put them in the database.
    '''
    for dirName, subdirList, fileList in os.walk(rootDir):
        print('Found directory: %s' % dirName)
        for fname in fileList:
            print('\t%s' % fname)
            fullName = dirName+'/'+fname
            print('\t\t%s'%  fullName)
            metaDb.insertFile(fullName)

def insertFile(fileName, metaDb):
    '''Insert the header information for 'fileName' into the
    MetadataDatabase metaDb, but only if it is a FITS file.
    '''
    if isFits(fileName):
        mdFits = MetadataFits(fileName)
        mdFits.scanFileAllHdus()
        metaDb.insertMetadataFits(mdFits)

def test():
   credFile = "~/.mysqlAuthLSST"
   creds = readCredentialFile(credFile, logging.getLogger("lsst.imgserv.metadatafits"))
   dbName = "{}_fitsTest".format(creds['user'])
   testFile = ("/lsst3/DC3/data/obs/ImSim/pt1_2/eimage/v886946741-fi/E000/R01/"
               "eimage_886946741_R01_S00_E000.fits.gz")
   problemFile = ("/lsst/home/jgates/test_metadata/lsst3/DC3/data/obs/ImSim/pt1_2/"
                  "replaced/raw/v886258731-fr/E000/R33/S21/"
                  "imsim_886258731_R33_S21_C12_E000.fits.gz")
   assert isFitsExt('stuf.fits') == True
   assert isFitsExt('thing.txt') == False
   assert isFitsExt('item.tx.gz') == False
   assert isFitsExt(testFile) == True
   assert isFits(testFile) == True

   # Destroy existing tables and re-create them
   dbTestDestroyCreate(credFile, dbName, "DELETE")

   # Open a connection to the database.
   metadataFits = dbOpen(credFile, dbName)

   # test a specific file
   metadataFits.insertFile(problemFile)
   quit()

   #root = '/lsst3/DC3/data/obs/ImSim/pt1_2/eimage/v886946741-fi/E000'
   root = '/lsst/home/jgates/test_metadata'
   directoryCrawl(root, metadataFits)
   metadataFits.close()


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s %(name)s %(levelname)s: %(message)s', 
        datefmt='%m/%d/%Y %I:%M:%S', 
        level=logging.DEBUG)
    test()




