#!/usr/bin/env python

# This script imports the allCountries.zip database into an SQL database
# Download the database from:
# http://www.geonames.org/export/
#
# Author: Gabriele Tozzi <gabriele@tozzi.eu>
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
#
# Requires
# - pyodbc (apt-get install python-pyodbc)
#

import sys
import time, datetime
import logging
import argparse, ConfigParser
import pyodbc
import zipfile

class main:
    
    NAME = 'importcountries'
    VERSION = '0.5'
    
    def Run(self):
        '''This is the entry point'''

        # Start timer
        self.__start = time.time()

        # Configure console logging
        logging.basicConfig(
            level = logging.INFO,
            format = '%(name)-12s: %(levelname)-8s %(message)s',
            datefmt = '%Y-%m-%d %H:%M:%S',
        )

        # Create logger
        self.__log = logging.getLogger()
        
        # Read the command line
        parser = argparse.ArgumentParser()
        parser.add_argument('configfile',
            help='the file to read config from')

        args = parser.parse_args()

        # Read the config file
        self.__config = ConfigParser.ConfigParser()
        self.__config.read(args.configfile)
        
        # Connect the database
        self.__db = pyodbc.connect('DRIVER={%s};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s' % (
            self.__config.get('db', 'driver'),
            self.__config.get('db', 'server'),
            self.__config.get('db', 'database'),
            self.__config.get('db', 'user'),
            self.__config.get('db', 'pass'),
        ))
        
        # If not using transactions, then enable autocommit
        transate = self.__config.getboolean('main', 'transate')
        if not transate:
            self.__log.info('Transactions disabled')
        
        # Create the tables
        self.__log.info('Creating tables...')
        self.__createTables(not transate)
    
        # Populate tables
        self.__log.info('Populating tables...')
        self.__populateTables(not transate)
        
        # Commit
        if transate:
            self.__log.info('Committing...')
            self.__db.commit()
    
    def __createTables(self, commit=False):
        ''' Create the SQL tables '''
        suffix =  self.__config.get('main', 'suffix')
        cursor = self.__db.cursor()
        
        q = """
            CREATE TABLE %sgeoname (
                geonameid INT IDENTITY NOT NULL,
                name VARCHAR(200) NOT NULL,
                asciiname VARCHAR(200) NULL,
                lat DECIMAL(15,12) NOT NULL,
                lng DECIMAL(16,12) NOT NULL,
                feat_class CHAR(1) NULL,
                feat_code VARCHAR(10) NULL,
                country_code CHAR(2) NOT NULL,
                admin1_code VARCHAR(20) NULL,
                admin2_code VARCHAR(80) NULL,
                admin3_code VARCHAR(20) NULL,
                admin4_code VARCHAR(20) NULL,
                population BIGINT NOT NULL,
                elevation INT NULL,
                gtopo30 INT NOT NULL,
                timezone VARCHAR(255) NULL,
                mod_date DATE NOT NULL
            )
        """ % (suffix, )
        cursor.execute(q)
        
        q = """
            CREATE NONCLUSTERED INDEX %sgeoname_feat_idx
            ON %sgeoname ( feat_class, feat_code )
        """ % (suffix, suffix, )
        cursor.execute(q)
        
        q = """
            CREATE NONCLUSTERED INDEX %sgeoname_admin_idx
            ON %sgeoname ( country_code, admin1_code, admin2_code, admin3_code, admin4_code )
        """ % (suffix, suffix, )
        cursor.execute(q)
    
        if commit:
            self.__db.commit()
    
    def __populateTables(self, commit=False):
        ''' Populate the tables '''
        suffix =  self.__config.get('main', 'suffix')
        cursor = self.__db.cursor()
        
        zf = zipfile.ZipFile(self.__config.get('main', 'source'), 'r')
        acf = zf.open('allCountries.txt', 'r')
        
        q = "SET IDENTITY_INSERT %sgeoname ON;" % suffix
        cursor.execute(q)
        
        q = """
            INSERT INTO %sgeoname(geonameid, name, asciiname, lat, lng, feat_class,
                feat_code, country_code, admin1_code, admin2_code, admin3_code,
                admin4_code, population, elevation, gtopo30,
                timezone, mod_date)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """ % (suffix, )
        
        i = 0
        size = zf.getinfo('allCountries.txt').file_size
        done = 0
        for line in acf:
            self.__log.debug(line)
            done += len(line)
            l = line.split("\t")
            if len(l) != 19:
                err = 'Unvalid line %s: len %s' % (i, len(l))
                self.__log.error(err)
                raise RuntimeError(err)
            
            # Clean data
            def clean(v):
                #Strip
                v = v.strip()
                #Nullize
                v = None if v=='' else v
                
                return v
            l = map(clean, l)
            if l[18]:
                l[18] = datetime.datetime.strptime(l[18], r'%Y-%m-%d')
            
            # Insert data
            p = (l[0],l[1],l[2],l[4],l[5],l[6],l[7],l[8],
                l[10],l[11],l[12],l[13],l[14],l[15],l[16],l[17],l[18])
            cursor.execute(q,p)
            
            if commit:
                self.__db.commit()

            # Calculate and show progress information every 1000 records
            if not i % 1000:
                progress = float(done) / size
                elapsed = time.time() - self.__start
                togo = float(elapsed) / progress
                self.__log.info(str(round(progress*100,2))+'%, ' + \
                    str(datetime.timedelta(seconds=int(elapsed))) + ' elapsed, ' + \
                    str(datetime.timedelta(seconds=int(togo))) + ' to go.')
            i += 1
        
        q = "SET IDENTITY_INSERT %sgeoname OFF;" % suffix
        cursor.execute(q)
    
if __name__ == '__main__':
    sys.exit(main().Run())
