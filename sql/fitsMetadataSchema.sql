-- LSST Data Management System
-- Copyright 2008, 2009, 2010 LSST Corporation.
-- 
-- This product includes software developed by the
-- LSST Project (http://www.lsst.org/).
--
-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation, either version 3 of the License, or
-- (at your option) any later version.
-- 
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
-- 
-- You should have received a copy of the LSST License Statement and 
-- the GNU General Public License along with this program.  If not, 
-- see <http://www.lsstcorp.org/LegalNotices/>.


-- LSST Database Schema, PT1_2 series
--
-- UCD definitions based on:
-- http://www.ivoa.net/Documents/cover/UCDlist-20070402.html


SET FOREIGN_KEY_CHECKS=0;


CREATE TABLE IF NOT EXISTS ZZZ_Db_Description 
    -- <descr>Internal table used for storing database description</descr>
(
    f VARCHAR(255),
        -- <descr>The schema file name.</descr>
    r VARCHAR(255)
        -- <descr>Captures information from 'git describe'.</descr>
) ENGINE=MyISAM;

INSERT INTO ZZZ_Db_Description(f) VALUES('fitsMetadataSchema.sql');


CREATE TABLE FitsFiles
    -- <descr>Table of basic FITS file information. Name, location, 
    -- number of HDUs 
    -- </desc>
(
    fitsFileId BIGINT       NOT NULL AUTO_INCREMENT, 
    fileName   VARCHAR(255) NOT NULL, 
    hdus       TINYINT      NOT NULL, 
    PRIMARY KEY (fitsFileId)
) ENGINE=InnoDB;

CREATE TABLE FitsKeyValues
    -- <descr>Table of FITS keyword and value pairs. </decr>
(
    fitsFileId  BIGINT      NOT NULL, 
    fitsKey     VARCHAR(8)  NOT NULL,
    hdu         TINYINT     NOT NULL,
    stringValue VARCHAR(90),
    intValue    INTEGER,
    doubleValue DOUBLE,
    FOREIGN KEY (fitsFileId) REFERENCES FitsFiles(fitsFileId)
) ENGINE=InnoDB;


CREATE TABLE FitsPositions 
    -- <descr>Table of RA and Dec position and exposure time.</descr>
(
    fitsFileId BIGINT  NOT NULL, 
    hdu        TINYINT NOT NULL, 
    equinox    DOUBLE,  
    pdec       DOUBLE, 
    pra        DOUBLE, 
    rotang     DOUBLE, 
    date       TIMESTAMP, 
    FOREIGN KEY (fitsFileId) REFERENCES FitsFiles(fitsFileId)
) ENGINE=InnoDB;

CREATE INDEX fits_key_fitsKey ON FitsKeyValues (fitsKey);

CREATE INDEX fits_pos_date ON FitsPositions (date);

CREATE INDEX fits_pos_ra ON FitsPositions (pra);

CREATE INDEX fits_pos_dec ON FitsPositions (pdec);
