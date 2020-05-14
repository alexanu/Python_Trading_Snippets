#!/usr/bin/env python
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_import_abcbourse.py
#
# Description: Import quotes from abcbourse.com
#
# The Original Code is iTrade code (http://itrade.sourceforge.net).
#
# The Initial Developer of the Original Code is	Gilles Dumortier.
#
# Portions created by the Initial Developer are Copyright (C) 2004-2008 the
# Initial Developer. All Rights Reserved.
#
# Contributor(s):
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; see http://www.gnu.org/licenses/gpl.html
#
# History       Rev   Description
# 2005-03-17    dgil  Wrote it from scratch
# ============================================================================

# ============================================================================
# Imports
# ============================================================================

# python system
import logging
import re
from datetime import *
from urllib import *
from httplib import *

# iTrade system
from itrade_logging import *
from itrade_quotes import *
from itrade_datation import Datation
from itrade_defs import *
from itrade_ext import *

# ============================================================================
# Import_ABCBourse()
#
# Notes (2005-03-17 night) :
#  - see ethereal traces in ethereal/ directory
#  - urlencode parameters are very important, even ImageButton* ones !
#  - ___VIEWSTATE environment variable shall be pass through
#    => GET the regular page to found ___VIEWSTATE content then POST using
#    this parameter
#  - headers content seems to be not very important
# ============================================================================

class Import_ABCBourse(object):
    def __init__(self):
        debug('Import_ABCBourse:__init__')
        self.m_host = "download.abcbourse.com"
        self.m_conn = None
        self.m_url = "/historiques.aspx?f=eb"
        self.m_viewstate = None

    def name(self):
        return 'abcbourse'

    def interval_year(self):
        return 2

    def connect(self):
        debug('Import_ABCBourse:connect to web site')
        try:
            self.m_conn = HTTPConnection(self.m_host,80)
        except:
            debug('Import_ABCBourse:unable to connect :-(')
            return False
        return True

    def disconnect(self):
        if self.m_conn:
            self.m_conn.close()
        self.m_conn = None

    def getstate(self):
        # check we have a connection
        if not self.m_conn:
            raise('Import_ABCBourse:no connection')
            return None

        # init headers
        headers = { "Keep-Alive":300, "Accept-Charset:":"ISO-8859-1", "Accept-Language": "en-us,en", "Accept": "text/html,text/plain", "Connection": "keep-alive", "Host": self.m_host }

        # GET the main download page
        try:
            self.m_conn.request("GET", self.m_url, None, headers)
            response = self.m_conn.getresponse()
        except:
            debug('Import_ABCBourse:GET failure')
            return None

        debug("status:%s reason:%s" %(response.status, response.reason))
        if response.status != 200:
            debug('Import_ABCBourse:status!=200')
            return None

        # search for the ___VIEWSTATE variable
        data = response.read()
        m = re.search(r'name=\"__VIEWSTATE\"\s*value=\"\S+\"', data)
        if m==None:
            debug('Import_ABCBourse:viewstate statement not found !')
            return None

        # extract the variable content
        m = m.group()[26:-1]
        self.m_viewstate = m

        return m

    def getdata(self,quote,datedebut=None,datefin=None):
        # check we have a connection
        if not self.m_conn:
            raise('Import_ABCBourse:no connection / missing connect() call !')
            return None
        # check we have a viewstate
        if not self.m_viewstate:
            raise('Import_ABCBourse:no viewstate / missing getstate() call !')
            return None

        if not datefin:
            datefin = date.today()

        if not datedebut:
            datedebut = date.today()

        if isinstance(datedebut,Datation):
            datedebut = datedebut.date()

        if isinstance(datefin,Datation):
            datefin = datefin.date()

        debug("Import_ABCBourse:getdata quote:%s begin:%s end:%s" % (quote,datedebut,datefin))

        # init params and headers
        params = urlencode({'__VIEWSTATE': self.m_viewstate, 'dayDeb': datedebut.day, 'monthDeb': datedebut.month, 'yearDeb': datedebut.year, 'dayFin': datefin.day, 'monthFin': datefin.month, 'yearFin': datefin.year, 'OneSico': 'on', 'txtOneSico': quote.isin(), 'dlFormat': 'e', 'listFormat': 'isin', 'ImageButton1.x': 25, 'ImageButton1.y': 10 })
        debug(params)
        headers = { "Keep-Alive":300, "Accept-Charset:":"ISO-8859-1", "Accept-Language": "en-us,en", "Content-type": "application/x-www-form-urlencoded", "Accept": "text/html,text/plain", "Connection": "keep-alive", "Host": self.m_host  }

        # POST the request
        try:
            self.m_conn.request("POST", self.m_url, params, headers)
            response = self.m_conn.getresponse()
        except:
            debug('Import_ABCBourse:POST failure')
            return None

        debug("status:%s reason:%s" %(response.status, response.reason))
        if response.status != 200:
            debug('Import_ABCBourse:status!=200')
            return None

        # returns the data
        data = response.read()

        # detect EBP file
        if data[:8]=="30111998":
            return data[8:]
        return ""

# ============================================================================
# Export me
# ============================================================================

try:
    ignore(gImportABC)
except NameError:
    gImportABC = Import_ABCBourse()

#registerImportConnector('EURONEXT','PAR',QLIST_ANY,QTAG_IMPORT,gImportABC,bDefault=False)
#registerImportConnector('ALTERNEXT','PAR',QLIST_ANY,QTAG_IMPORT,gImportABC,bDefault=False)
#registerImportConnector('PARIS MARCHE LIBRE','PAR',QLIST_ANY,QTAG_IMPORT,gImportABC,bDefault=False)

# ============================================================================
# Test ME
#
# Notes (2005-03-17 night) :
#  - see ethereal traces in ethereal/ directory
#  - urlencode parameters are very important, even ImageButton* ones !
#  - ___VIEWSTATE environment variable shall be pass through
#    => GET the regular page to found ___VIEWSTATE content then POST using
#    this parameter
#  - headers content seems to be not very important
# ============================================================================

def test(ticker,d):
    if gImportABC.connect():

        state = gImportABC.getstate()
        if state:
            debug("state=%s" % (state))

            quote = quotes.lookupTicker(ticker,'EURONEXT')
            data = gImportABC.getdata(quote,d)
            if data!=None:
                if data:
                    debug(data)
                else:
                    debug("nodata")
            else:
                print "getdata() failure :-("
        else:
            print "getstate() failure :-("

        gImportABC.disconnect()
    else:
        print "connect() failure :-("

if __name__=='__main__':
    setLevel(logging.INFO)

    # never failed - fixed date
    print "15/03/2005"
    test('OSI',date(2005,3,15))

    # never failed except week-end
    print "yesterday-today :-("
    test('OSI',date.today()-timedelta(1))

    # always failed
    print "tomorrow :-)"
    test('OSI',date.today()+timedelta(1))

# ============================================================================
# That's all folks !
# ============================================================================
