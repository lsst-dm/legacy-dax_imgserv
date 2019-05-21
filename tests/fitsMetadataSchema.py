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

schemaToCreate = (
"""
CREATE TABLE FitsFiles
    -- <descr>Table of basic FITS file information. Name, location,
    -- number of HDUs
    -- </descr>
(
    fitsFileId BIGINT       NOT NULL AUTO_INCREMENT,
    fileName   VARCHAR(255) NOT NULL,
    hduCount   TINYINT      NOT NULL,
    PRIMARY KEY(fitsFileId)
) ENGINE=InnoDB
""",

"""CREATE TABLE FitsKeyValues
    -- <descr>Table of FITS keyword and value pairs. </descr>
(
    fitsFileId  BIGINT      NOT NULL,
    fitsKey     VARCHAR(8)  NOT NULL,
    hdu         TINYINT     NOT NULL,
    stringValue VARCHAR(1000),
    intValue    INTEGER,
    doubleValue DOUBLE,
    lineNum     INTEGER,
    comment     VARCHAR(90),
    INDEX IDX_fitsKeyVal_fitsFileId(fitsFileId),
    INDEX IDX_fitsKeyVal_fitsKey(fitsKey)
) ENGINE=InnoDB
""",

"""CREATE TABLE FitsPositions
    -- <descr>Table of RA and Dec position and exposure time.</descr>
(
    fitsFileId BIGINT  NOT NULL,
    hdu        TINYINT NOT NULL,
    equinox    DOUBLE,
    pDec       DOUBLE,
    pRa        DOUBLE,
    rotAng     DOUBLE,
    pDate      TIMESTAMP,
    INDEX IDX_fitsPos_fitsFileId(fitsFileId),
    INDEX IDX_fitsPos_date(pDate),
    INDEX IDX_fitsPos_ra(pRa),
    INDEX IDX_fitsPos_dec(pDec)
) ENGINE=InnoDB
""",

"""ALTER TABLE FitsKeyValues ADD CONSTRAINT FK_fitsKeyVal_fitsFileId
    FOREIGN KEY (fitsFileId) REFERENCES FitsFiles(fitsFileId)
""",

"""ALTER TABLE FitsPositions ADD CONSTRAINT FK_fitsPos_fitsFileId
    FOREIGN KEY (fitsFileId) REFERENCES FitsFiles(fitsFileId)
"""

)

