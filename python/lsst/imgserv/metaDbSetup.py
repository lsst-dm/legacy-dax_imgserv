

import lsst.cat


from lsst.cat.MySQLBase import MySQLBase
import os
import subprocess
import sys

import MySQLdb


class DbMetaSetup(MySQLBase):
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
    print setup
    dbSetup = DbMetaSetup(dbHostName="lsst10.ncsa.illinois.edu",
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
    
