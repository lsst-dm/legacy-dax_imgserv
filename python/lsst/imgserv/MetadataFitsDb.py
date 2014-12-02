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

import gzip
import MySQLdb

import lsst.afw
import lsst.afw.image as afwImage
import lsst.daf.base as dafBase



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
             print "Reading", name, lineNum, data, comment
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
                           ("rotang","ROTANG"), ("date","DATE"))

    def _insert(self):
        # Figure out the date
        columns = {}
        if ('DATE', self._hdu) in self._entries:
            dt = self._entries[('DATE', self._hdu)][0]
            dt = dt.replace('T',' ')
            print dt
            if isDateFormatValid(dt):
                columns['date'] = dt
        found = False
        # Set equinox, use EQUINOX if it exists, otherwise use EPOC (deprecated)
        if ('EQUINOX', self._hdu) in self._entries:
            eq = self._entries[('EQUINOX', self._hdu)][0]
            print "EQUINNOX eq=", eq
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
            if column != 'date' and column != 'equinox':
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
            print sql # TODO DELETE
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

    def _createTables(self):    # TODO delete this function
        #InnoDB
        fileTable = \
            ("CREATE TABLE FitsFiles ("
             "fitsFileId BIGINT       NOT NULL AUTO_INCREMENT, "
             "fileName   VARCHAR(255) NOT NULL, "
             "hdus       TINYINT      NOT NULL, "
             "PRIMARY KEY (fitsFileId)"
             ")") # TODO add hash?, timestamp?
        valuesTable = \
            ("CREATE TABLE FitsKeyValues ("
             "fitsFileId  BIGINT      NOT NULL, "
             "fitsKey     VARCHAR(8)  NOT NULL, "
             "hdu         TINYINT     NOT NULL, "
             "stringValue VARCHAR(255), "
             "intValue    INTEGER, "
             "doubleValue DOUBLE, "
             "lineNum     INTEGER, "
             "comment     VARCHAR(90), "
             "FOREIGN KEY (fitsFileId) REFERENCES FitsFiles(fitsFileId)"
             ")")
        positionTable = \
            ("CREATE TABLE FitsPositions ("
             "fitsFileId BIGINT  NOT NULL, "
             "hdu        TINYINT NOT NULL, "
             "equinox    DOUBLE, " 
             "pdec       DOUBLE, "
             "pra        DOUBLE, "
             "rotang     DOUBLE, "
             "date       TIMESTAMP, "
             "FOREIGN KEY (fitsFileId) REFERENCES FitsFiles(fitsFileId)"
             ")")
        indexFitsKey     = "CREATE INDEX fits_key_fitsKey ON FitsKeyValues (fitsKey)"
        indexFitsPosDate = "CREATE INDEX fits_pos_date ON FitsPositions (date)"
        indexFitsPosRA   = "CREATE INDEX fits_pos_ra ON FitsPositions (pra)"
        indexFitsPosDec  = "CREATE INDEX fits_pos_dec ON FitsPositions (pdec)"
        cursor = self._connect.cursor()
        for sql in ( fileTable, valuesTable, positionTable, indexFitsKey,
                     indexFitsPosDate, indexFitsPosRA, indexFitsPosDec ):
            try:
                cursor.execute(sql)
                print "created {}".format(sql)
            except MySQLdb.Error as err:
                print "ERROR MySQL {} --  {}".format(err, sql)
                quit()
        cursor.close()

    def _dropTables(self, code):  # TODO delete this function
        '''For testing purposes only'''
        if code == "DELETE":
            cursor = self._connect.cursor()
            for tbl in ("FitsKeyValues", "FitsPositions", "FitsFiles"):
                try:
                    cursor.execute("DROP TABLE {}".format(tbl))
                    print "dropped", tbl
                except MySQLdb.Error as err:
                    print "ERROR MySQL {} --  {}".format(err, tbl)
            cursor.close()
        else:
            print "Keeping all tables."

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
        print sql #TODO DELETE
        cursor.execute(sql)

    def insertFile(self, fileName):
        '''Insert the header information for 'fileName' into the
        MetadataDatabase metaDb, but only if it is a FITS file.
        '''
        if isFits(fileName):
            mdFits = MetadataFits(fileName)
            mdFits.scanFileAllHdus()
            self.insertMetadataFits(mdFits)
            print mdFits.dump()

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
            print sql # TODO delete
            cursor.execute(sql)
            r = cursor.fetchall()
            # Nothing found, so it needs to be added.
            if len(r) < 1:
                #If this fails for any reason, we do not want the database altered.
                cursor.execute("START TRANSACTION")
                cursor.execute("SET autocommit = 0")
                sql = ("INSERT INTO FitsFiles (fileName, hdus) "
                       "VALUES ('{}', {})".format(fileName, hdus))
                print sql # TODO DELETE
                cursor.execute(sql)
                lastFitsFileId = cursor.lastrowid
                for key, entry in entries.iteritems():
                    value   = entry[0]
                    lineNum = entry[1]
                    comment = entry[2]
                    # TODO Temporary hack - merge tuple into single string
                    #if isinstance(value, tuple):
                    #    valStrs = map(str, value)
                    #    s = ", "
                    #    value = s.join(valStrs)
                    #self._insertFitsValue(cursor, lastFitsFileId, key, value, lineNum, comment)
                    # Or put in one entry for each element of the tuple
                    if isinstance(value, tuple):
                        num = lineNum
                        for v in value:
                            # scanFile should be skipping line numbers when it sees a tuple.
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
            print "ERROR MySQLdb {} -- {}".format(err, sql)
            quit() # TODO delete this line, for now it is good to stop and examine these.
        cursor.close()

def dbOpenTest():
    # Hard coded credentials will be replaced with readCredentialFile.
    dbHost = "lsst10.ncsa.illinois.edu"
    dbPort = 3306
    dbUser ="jgates"
    dbPass = "squid7sql"
    dbName = "jgates_test1"
    mdFits = MetadataFitsDb(dbHost=dbHost, dbPort=dbPort,
                            dbUser=dbUser, dbPasswd=dbPass,
                            dbName=dbName)
    return mdFits

def dbTest():
    '''Open the test database, delete tables and re-create.
    '''
    mdFits = dbOpenTest()
    mdFits._dropTables("DELETE")
    mdFits._createTables()
    mdFits.showTables()
    mdFits.close()

def isFitsExt(fileName):
    '''Return True if the file extension reasonable for a FITS file.
    '''
    nameSplit = fileName.split('.')
    #print nameSplit
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
   dbTest()

   # Open a connection to the database.
   metadataFits = dbOpenTest()

   # test a specific file
   metadataFits.insertFile(problemFile)
   quit()

   #root = '/lsst3/DC3/data/obs/ImSim/pt1_2/eimage/v886946741-fi/E000'
   root = '/lsst/home/jgates/test_metadata'
   directoryCrawl(root, metadataFits)
   metadataFits.close()


if __name__ == "__main__":
    test()




