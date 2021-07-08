#!/usr/bin/env python
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_liveupdate_yahoo.py
#
# Description: Live update quotes from yahoo.com
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
# 2005-10-17    dgil  Wrote it from scratch
# ============================================================================

# ============================================================================
# Imports
# ============================================================================

# python system
import logging
import re
import thread
import string
import time
import pytz
from pytz import timezone
# iTrade system
import itrade_config
from itrade_logging import *
from itrade_quotes import *
from itrade_defs import *
from itrade_ext import *
from itrade_market import yahooTicker,yahooUrl,convertConnectorTimeToPlaceTime
from itrade_connection import ITradeConnection
import itrade_config

# ============================================================================
# LiveUpdate_yahoo()
#
# ============================================================================

class LiveUpdate_yahoo(object):
    def __init__(self):
        debug('LiveUpdate_yahoo:__init__')

        self.m_connected = False
        self.m_livelock = thread.allocate_lock()
        self.m_dateindice = {}
        self.m_clock = {}
        self.m_dcmpd = {}
        self.m_lastclock = 0
        self.m_lastdate = "20070101"

        self.m_connection = ITradeConnection(cookies = None,
                                           proxy = itrade_config.proxyHostname,
                                           proxyAuth = itrade_config.proxyAuthentication,
                                           connectionTimeout = itrade_config.connectionTimeout
                                           )

    # ---[ reentrant ] ---
    def acquire(self):
        self.m_livelock.acquire()

    def release(self):
        self.m_livelock.release()

    # ---[ properties ] ---

    def name(self):
        # name of the connector
        return 'yahoo'

    def delay(self):
        # delay in minuts to get a live data
        # put 0 if no delay (realtime)
        return 15

    def timezone(self):
        # timezone of the livedata (see pytz all_timezones)
        return "EST"

    # ---[ connexion ] ---

    def connect(self):
        return True

    def disconnect(self):
        pass

    def alive(self):
        return self.m_connected

    # ---[ state ] ---

    def getstate(self):
        # no state
        return True

    # ---[ code to get data ] ---

    def yahooDate (self,date):
        # Date part is easy.
        sdate = string.split (date[1:-1], '/')
        month = string.atoi (sdate[0])
        day = string.atoi (sdate[1])
        year = string.atoi (sdate[2])

        return "%4d%02d%02d" % (year,month,day)

    def convertClock(self,place,clock,date):
        clo = clock[:-2]
        min = clo[-2:]
        hour = clo[:-3]
        val = (int(hour)*60) + int(min)
        per = clock[-2:]
        if per=='pm':
            if int(hour) < 12:
                val = val + 12*60
        elif per == 'am':
            if int(hour) >= 12:
                val = val - 12*60

        # yahoo return EDT OR EST time
        eastern = timezone('US/Eastern')
        mdatetime = datetime(int(date[0:4]),int(date[4:6]),int(date[6:8]),val/60,val%60)
        loc_dt = eastern.localize(mdatetime)
        if str(loc_dt.strftime('%Z')) == 'EDT':
            val = val-60
            if val <= 0:
                val = (12*60)-60
                
        #print clock,clo,hour,min,val,per,date

        if val>self.m_lastclock and date>=self.m_lastdate:
            self.m_lastdate = date
            self.m_lastclock = val

        # convert from connector timezone to market place timezone
        mdatetime = datetime(int(date[0:4]),int(date[4:6]),int(date[6:8]),val/60,val%60)
        mdatetime = convertConnectorTimeToPlaceTime(mdatetime,self.timezone(),place)
        return "%d:%02d" % (mdatetime.hour,mdatetime.minute)

    def getdata(self,quote):
        debug("LiveUpdate_yahoo:getdata quote:%s " % quote)
        self.m_connected = False

        sname = yahooTicker(quote.ticker(),quote.market(),quote.place())

        if sname[0]=='^':
            ss = "%5E" + sname[1:]
        else:
            ss = sname

            
        query = (
            ('s', ss),
            ('f', 'sl1d1t1c1ohgv'),
            ('e', '.csv'),
        )
        query = map(lambda (var, val): '%s=%s' % (var, str(val)), query)
        query = string.join(query, '&')
        url = yahooUrl(quote.market(),live=True) + '?' + query

        debug("LiveUpdate_yahoo:getdata: url=%s",url)
        try:
            data=self.m_connection.getDataFromUrl(url)[:-2] # Get rid of CRLF
        except:
            debug('LiveUpdate_yahoo:unable to connect :-(')
            return None

        # pull data
        s400 = re.search(r"400 Bad Request", data, re.IGNORECASE|re.MULTILINE)
        if s400:
            if itrade_config.verbose:
                info('unknown %s quote (400 Bad Request) from Yahoo' % (quote.ticker()))
            return None


        sdata = string.split (data, ',')
        if len (sdata) < 9:
            if itrade_config.verbose:
                info('invalid data (bad answer length) for %s quote' % (quote.ticker()))
            return None

        #print sdata

        # connexion / clock
        self.m_connected = True

        # store for later use
        key = quote.key()

        sclock = sdata[3][1:-1]
        if sclock=="N/A" or sdata[2]=='"N/A"' or len(sclock)<5:
            if itrade_config.verbose:
                info('invalid datation for %s : %s %s' % (quote.ticker(),sclock,sdata[2]))
                #print sdata
            return None

        # start decoding
        symbol = sdata[0][1:-1]
        if symbol != sname:
            if itrade_config.verbose:
                info('invalid ticker : ask for %s and receive %s' % (sname,symbol))
            return None

        # date
        try:
            date = self.yahooDate(sdata[2])
            self.m_dcmpd[key] = sdata
            self.m_clock[key] = self.convertClock(quote.place(),sclock,date)
            self.m_dateindice[key] = sdata[2].replace('"','')
        except ValueError:
            if itrade_config.verbose:
                info('invalid datation for %s : %s %s' % (quote.ticker(),sclock,sdata[2]))
            return None

        # decode data
        value = string.atof (sdata[1])

        if (sdata[4]=='N/A'):
            debug('invalid change : N/A')
            change = 0.0
            return None
        else:
            change = string.atof (sdata[4])
        if (sdata[5]=='N/A'):
            debug('invalid open : N/A')
            open = 0.0
            return None
        else:
            open = string.atof (sdata[5])
        if (sdata[6]=='N/A'):
            debug('invalid high : N/A')
            high = 0.0
            return None
        else:
            high = string.atof (sdata[6])
        if (sdata[7]=='N/A'):
            debug('invalid low : N/A')
            low = 0.0
            return None
        else:
            low = string.atof (sdata[7])

        volume = string.atoi (sdata[8])
        if volume<0:
            debug('volume : invalid negative %d' % volume)
            return None
        if volume==0 and quote.list()!=QLIST_INDICES:
            debug('volume : invalid zero value %d' % volume)
            return None
        else:
            if value-change <= 0:
                return None
            else:
                percent = (change / (value - change))*100.0

        # ISIN;DATE;OPEN;HIGH;LOW;CLOSE;VOLUME;PERCENT;PREVIOUS
        data = (
          key,
          date,
          open,
          high,
          low,
          value,
          volume,
          percent,
          (value-change)
        )
        data = map(lambda (val): '%s' % str(val), data)
        data = string.join(data, ';')

        # temp: hunting an issue (SF bug 1848473)
        # if itrade_config.verbose:
        #    print data

        return data

    # ---[ cache management on data ] ---

    def getcacheddata(self,quote):
        # no cache
        return None

    def iscacheddataenoughfreshq(self):
        # no cache
        return False

    def cacheddatanotfresh(self):
        # no cache
        pass

    # ---[ notebook of order ] ---

    def hasNotebook(self):
        return True

    def currentNotebook(self,quote):
        #
        key = quote.key()

        if not self.m_dcmpd.has_key(key):
            # no data for this quote !
            return [],[]
        d = self.m_dcmpd[key]

        #buy = []
        #buy.append([0,0,d[9]])

        #sell = []
        #sell.append([0,0,d[10]])

        #return buy,sell
        return [],[]

    # ---[ status of quote ] ---

    def hasStatus(self):
        return itrade_config.isConnected()

    def currentStatus(self,quote):
        #
        key = quote.key()
        if not self.m_dcmpd.has_key(key):
            # no data for this quote !
            return "UNKNOWN","::","0.00","0.00","::"
        d = self.m_dcmpd[key]

        st = 'OK'
        cl = '::'
        return st,cl,"-","-",self.m_clock[key]

    def currentClock(self,quote=None):
        if quote==None:
            if self.m_lastclock == 0:
                return "::"
            # hh:mm
            return "%d:%02d" % (self.m_lastclock/60,self.m_lastclock%60)

        key = quote.key()
        if not self.m_clock.has_key(key):
            # no data for this quote !
            return "::"
        else:
            return self.m_clock[key]

    def currentDate(self,quote=None):
        key = quote.key()
        if not self.m_dateindice.has_key(key):
            # no date for this quote !
            return "----"
        else:
            # convert yahoo date
            conv=time.strptime(self.m_dateindice[key],"%m/%d/%Y")
            return time.strftime("%d/%m/%Y",conv)

    def currentTrades(self,quote):
        # clock,volume,value
        return None

    def currentMeans(self,quote):
        # means: sell,buy,last
        return "-","-","-"

# ============================================================================
# Export me
# ============================================================================

try:
    ignore(gLiveYahoo)
except NameError:
    gLiveYahoo = LiveUpdate_yahoo()

registerLiveConnector('NASDAQ','NYC',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)
registerLiveConnector('NYSE','NYC',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)
registerLiveConnector('AMEX','NYC',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)
registerLiveConnector('OTCBB','NYC',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)
registerLiveConnector('LSE','LON',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('ASX','SYD',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('TORONTO VENTURE','TOR',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)
registerLiveConnector('TORONTO EXCHANGE','TOR',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('LSE SETS','LON',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)
registerLiveConnector('LSE SETSqx','LON',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)
registerLiveConnector('LSE SEAQ','LON',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('MILAN EXCHANGE','MIL',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('SWISS EXCHANGE','XSWX',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)
registerLiveConnector('SWISS EXCHANGE','XVTX',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('EURONEXT','PAR',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)
registerLiveConnector('EURONEXT','BRU',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)
registerLiveConnector('EURONEXT','AMS',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)
registerLiveConnector('EURONEXT','LIS',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('EURONEXT','PAR',QLIST_INDICES,QTAG_DIFFERED,gLiveYahoo,bDefault=True)
registerLiveConnector('EURONEXT','AMS',QLIST_INDICES,QTAG_DIFFERED,gLiveYahoo,bDefault=True)
registerLiveConnector('EURONEXT','BRU',QLIST_INDICES,QTAG_DIFFERED,gLiveYahoo,bDefault=True)
registerLiveConnector('EURONEXT','LIS',QLIST_INDICES,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('ALTERNEXT','PAR',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)
registerLiveConnector('ALTERNEXT','AMS',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)
registerLiveConnector('ALTERNEXT','BRU',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)
registerLiveConnector('ALTERNEXT','LIS',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('PARIS MARCHE LIBRE','PAR',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)
registerLiveConnector('PARIS MARCHE LIBRE','BRU',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('BRUXELLES MARCHE LIBRE','BRU',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('IRISH EXCHANGE','DUB',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)
registerLiveConnector('MADRID EXCHANGE','MAD',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('FRANKFURT EXCHANGE','FRA',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('STOCKHOLM EXCHANGE','STO',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('COPENHAGEN EXCHANGE','CSE',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('OSLO EXCHANGE','OSL',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('SAO PAULO EXCHANGE','SAO',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('HONG KONG EXCHANGE','HKG',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('SHANGHAI EXCHANGE','SHG',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('SHENZHEN EXCHANGE','SHE',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('NATIONAL EXCHANGE OF INDIA','NSE',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('BOMBAY EXCHANGE','BSE',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('NEW ZEALAND EXCHANGE','NZE',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('BUENOS AIRES EXCHANGE','BUE',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('MEXICO EXCHANGE','MEX',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('SINGAPORE EXCHANGE','SGX',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('KOREA STOCK EXCHANGE','KRX',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('KOREA KOSDAQ EXCHANGE','KOS',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('WIENER BORSE','WBO',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

registerLiveConnector('TAIWAN STOCK EXCHANGE','TAI',QLIST_ANY,QTAG_DIFFERED,gLiveYahoo,bDefault=True)

# ============================================================================
# Test ME
#
# ============================================================================

def test(ticker):
    if gLiveYahoo.iscacheddataenoughfreshq():
        data = gLiveYahoo.getcacheddata(ticker)
        if data:
            debug(data)
        else:
            debug("nodata")

    elif gLiveYahoo.connect():

        state = gLiveYahoo.getstate()
        if state:
            debug("state=%s" % (state))

            quote = quotes.lookupTicker(ticker,'NASDAQ')
            data = gLiveYahoo.getdata(Quote)
            if data!=None:
                if data:
                    info(data)
                else:
                    debug("nodata")
            else:
                print "getdata() failure :-("
        else:
            print "getstate() failure :-("

        gLiveYahoo.disconnect()
    else:
        print "connect() failure :-("

if __name__=='__main__':
    setLevel(logging.INFO)

    print 'live %s' % date.today()
    test('AAPL')

# ============================================================================
# That's all folks !
# ============================================================================
