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

# The purpose of this is read and execute the SQL scripts for the 
# Metadata Store database. It is essentially copied from lsst.cat.


import lsst.cat


from lsst.cat.MySQLBase import MySQLBase
import os
import subprocess
import sys

import MySQLdb


class MetadataDbSetup(MySQLBase):
    """
    This file contains a set of utilities to manage per-user databases
    """

    def __init__(self, dbHostName, portNo, userName, userPwd):
        MySQLBase.__init__(self, dbHostName, portNo)

        if userName == "":
            raise RuntimeError("Invalid (empty) userName")
        self.userName = userName
        self.userPwd = userPwd
        self.userDb = '%s_test1' % userName


    def setupUserDb(self, dbScripts):
        """
        Sets up user database (creates and loads stored procedures/functions).
        Database name: <userName>_dev.
        If the database exists, it will remove it first.
        """
        print self.setupUserDb
        # Check that scripts exist
        for f in dbScripts:
            if not os.path.exists(f):
                raise RuntimeError("Can't find file '%s'" % f)

        # (re-)create database
        print self.userName, self.userPwd, self.dbHostName, self.userDb
        self.connect(self.userName, self.userPwd, dbName=self.userDb)
        if self.dbExists(self.userDb):
            self.dropDb(self.userDb)
        self.createDb(self.userDb)
        self.disconnect()

        # load the scripts
        for f in dbScripts:
            self.loadSqlScript(f, self.userName, self.userPwd, self.userDb)


    def dropUserDb(self):
        self.connect(self.userName, self.userPwd)
        if self.dbExists(self.userDb):
            self.dropDb(self.userDb)
        self.disconnect()





def setup():
    # Hard coded credentials are temporary.
    dbSetup = MetadataDbSetup(dbHostName="lsst10.ncsa.illinois.edu",
                      portNo   = 3306,
                      userName = "jgates",
                      userPwd  = "squid7sql")
 
    scripts_temp = ["fitsMetadataSchema.sql"]
    sqlDir  = "../../../sql"
    scripts = []
    for script in scripts_temp:
        scripts.append(os.path.join(sqlDir, script))
    dbSetup.setupUserDb(scripts)




if __name__ == "__main__":
    setup()
    
