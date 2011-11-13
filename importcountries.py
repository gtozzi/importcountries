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

import sys, os
import time, datetime
import logging
import argparse, ConfigParser
import pyodbc
import zipfile
from collections import OrderedDict

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
        if self.__config.getboolean('main', 'create'):
            self.__log.info('Creating tables...')
            self.__createTables(not transate)
    
        # Populate tables
        for src in self.__config.get('main', 'import').split(','):
            self.__log.info('Populating ' + src + '...')
            self.__populateTable(src, not transate)
        
        # Commit
        if transate:
            self.__log.info('Committing...')
            self.__db.commit()
    
    def __createTables(self, commit=False):
        ''' Create the SQL tables '''
        suffix =  self.__config.get('main', 'suffix')
        cursor = self.__db.cursor()
        
        q = """
            CREATE TABLE %scountries (
                id CHAR(2) NOT NULL,
                iso3 CHAR(3) NOT NULL,
                iso_numeric SMALLINT NOT NULL,
                fips CHAR(2),
                name VARCHAR(255) NOT NULL,
                capital VARCHAR(255) NULL,
                area BIGINT NULL,
                population INT NOT NULL,
                continent CHAR(2) NOT NULL,
                tld VARCHAR(10),
                currency_code CHAR(3) NULL,
                currency_name VARCHAR(25) NULL,
                phone VARCHAR(25),
                postal_code_format VARCHAR(25),
                postal_code_regex VARCHAR(255),
                CONSTRAINT %scountries_pk PRIMARY KEY (id)
            )
        """ % (suffix, suffix)
        cursor.execute(q)
        
        q = """
            CREATE TABLE %sgeoadmins (
                id INT IDENTITY NOT NULL,
                country_code CHAR(2) NOT NULL,
                admin1_code VARCHAR(20) NOT NULL,
                admin2_code VARCHAR(80),
                admin3_code VARCHAR(20),
                admin4_code VARCHAR(20),
                name VARCHAR(200),
                asciiname VARCHAR(200) NOT NULL,
                CONSTRAINT %sgeoadmins_pk PRIMARY KEY (id)
            )
        """ % (suffix, suffix)
        cursor.execute(q)
        
        q = """
            CREATE UNIQUE  NONCLUSTERED INDEX %sgeoadmins_code_idx
            ON %sgeoadmins
            ( country_code, admin1_code, admin2_code, admin3_code, admin4_code )
        """ % (suffix, suffix, )
        cursor.execute(q)
        
        q = """
            CREATE TABLE %sgeoname (
                id INT IDENTITY NOT NULL,
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
    
    def __populateTable(self, name, commit=False):
        ''' Populate the given table '''
        suffix = self.__config.get('main', 'suffix')
        filename = self.__config.get('sources', name)
        cursor = self.__db.cursor()
        
        (root, ext) = os.path.splitext(os.path.basename(filename))
        if ext == '.zip':
            zf = zipfile.ZipFile(filename, 'r')
            acf = zf.open(root+'.txt', 'r')
            size = zf.getinfo(root+'.txt').file_size
        elif ext == '.txt':
            acf = open(filename, 'r')
            size = os.path.getsize(filename)
        else:
            raise RuntimeError('Unknown file extension: ' + ext)
        
        # Determine destination table and values map
        if root == 'allCountries':
            table = 'geoname'
            valmap = OrderedDict([
                ('id', 0),
                ('name', 1),
                ('asciiname', 2),
                ('lat', 4),
                ('lng', 5),
                ('feat_class', 6),
                ('feat_code', 7),
                ('country_code', 8),
                ('admin1_code', 10),
                ('admin2_code', 11),
                ('admin3_code', 12),
                ('admin4_code', 13),
                ('population', 14),
                ('elevation', 15),
                ('gtopo30', 16),
                ('timezone', 17),
                ('mod_date', (18, lambda v: datetime.datetime.strptime(v, r'%Y-%m-%d'))),
            ])
            linelen = 19
            identity = True
        elif root == 'countryInfo':
            table = 'countries'
            valmap = OrderedDict([
                ('id', 0),
                ('iso3', 1),
                ('iso_numeric', 2),
                ('fips', 3),
                ('name', 4),
                ('capital', 5),
                ('area', (6, lambda v: int(float(v)))),
                ('population', 7),
                ('continent', 8),
                ('tld', 9),
                ('currency_code', 10),
                ('currency_name', 11),
                ('phone', 12),
                ('postal_code_format', 13),
                ('postal_code_regex', 14),
            ])
            linelen = 19
            identity = False
        elif root == 'admin1CodesASCII' or root == 'admin2Codes':
            table = 'geoadmins'
            valmap = OrderedDict([
                ('country_code', (0, lambda v: v.split('.')[0] if v!=None and len(v.split('.'))>0 else None)),
                ('admin1_code', (0, lambda v: v.split('.')[1] if v!=None and len(v.split('.'))>1 else None)),
                ('admin2_code', (0, lambda v: v.split('.')[2] if v!=None and len(v.split('.'))>2 else None)),
                ('admin3_code', (0, lambda v: v.split('.')[3] if v!=None and len(v.split('.'))>3 else None)),
                ('admin4_code', (0, lambda v: v.split('.')[4] if v!=None and len(v.split('.'))>4 else None)),
                ('name', 1),
                ('asciiname', 2),
            ])
            linelen = 4
            identity = False
        else:
            raise RuntimeError('Uknown file: ' + root)
        
        if identity:
            q = "SET IDENTITY_INSERT %s%s ON" % (suffix, table)
            cursor.execute(q)
        
        q = """
            INSERT INTO %s%s(%s) VALUES(%s)
        """ % (suffix, table, ','.join(valmap.keys()), ','.join(['?',]*len(valmap)))

        i = 0
        done = 0
        for line in acf:
            #Ignore lines starting with #
            if line[0] == '#':
                continue
            self.__log.debug(line)
            done += len(line)
            l = line.split("\t")
            if len(l) != linelen:
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
            
            # Build parameters
            p = []
            for k in valmap.keys():
                d = valmap.get(k)
                if type(d) == int:
                    p.append(l[d])
                elif type(d) == tuple:
                    if l[d[0]] == None:
                        p.append(None)
                    else:
                        p.append(d[1](l[d[0]]))
                else:
                    raise RuntimeError('Error in valmap')
            
            # Insert data
            try:
                cursor.execute(q,p)
            except Exception as e:
                self.__log.critical('Query: ' + q)
                self.__log.critical('Params: ' + str(p))
                self.__log.critical('SQL exception in line ' + str(i+1) + '!')
                self.__log.critical(str(line))
                raise e
            
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
        
        if identity:
            q = "SET IDENTITY_INSERT %s%s OFF" % (suffix, table)
            cursor.execute(q)
    
if __name__ == '__main__':
    sys.exit(main().Run())
