#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_liveupdate_yahoo_japon.py
#
# Description: Live update quotes from http://quote.yahoo.co.jp/
#
# The Original Code is iTrade code (http://itrade.sourceforge.net).
#
# The Initial Developer of the Original Code is	Gilles Dumortier.
# New code for Yahoo_Japan is from Michel Legrand.

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
import urllib

# iTrade system
import itrade_config
from itrade_logging import *
from itrade_quotes import *
from itrade_defs import *
from itrade_ext import *
from itrade_market import yahooTicker,yahooUrlJapan,convertConnectorTimeToPlaceTime
from itrade_connection import ITradeConnection

# ============================================================================
# LiveUpdate_yahoojp()
#
# ============================================================================

class LiveUpdate_yahoojp(object):
    def __init__(self,market='TOKYO EXCHANGE'):
        debug('LiveUpdate_yahoojp:__init__')
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


    def splitLines(self,buf):
        lines = string.split(buf, '\n')
        lines = filter(lambda x:x, lines)
            
        def removeCarriage(s):
            if s[-1]=='\r':
                return s[:-1]
            else:
                return s
        lines = [removeCarriage(l) for l in lines]
        return lines


    # ---[ reentrant ] ---
    def acquire(self):
        self.m_livelock.acquire()

    def release(self):
        self.m_livelock.release()

    # ---[ properties ] ---

    def name(self):
        # name of the connector
        return 'yahoojp'

    def delay(self):
        # delay in minuts to get a live data
        # put 0 if no delay (realtime)
        return 15

    def timezone(self):
        # timezone of the livedata (see pytz all_timezones)
        return "Asia/Tokyo"

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
        min = clock[-2:]
        hour = clock[:-3]
        val = (int(hour)*60) + int(min)

        if val>self.m_lastclock and date>=self.m_lastdate:
            self.m_lastdate = date
            self.m_lastclock = val

        # convert from connector timezone to market place timezone
        mdatetime = datetime(int(date[0:4]),int(date[4:6]),int(date[6:8]),val/60,val%60)
        mdatetime = convertConnectorTimeToPlaceTime(mdatetime,self.timezone(),place)
        return "%d:%02d" % (mdatetime.hour,mdatetime.minute)

    def getdata(self,quote):

        sname = yahooTicker(quote.ticker(),quote.market(),quote.place())
        ss = sname
        url = yahooUrlJapan(quote.market(),live=True) + '?' +'s=%s&d=v2' % (ss)

        debug("LiveUpdate_yahoojp:getdata: url=%s",url)
        
        try:
            
            data=self.m_connection.getDataFromUrl(url)
            data = data.replace('</td>','\n')
            lines = self.splitLines(data)

            # returns the data
            
            for line in lines:
                if 'uncompressed' in line:
                    year = line[line.find('JST ')+4:line.find(' -->')]
                else : year = '0000'

                   
                
            
            #sdata =[]
            ch = '<td nowrap align=center>'
            i = 0
            n = 0
                #typical lines
                #1 datetime  <td nowrap align=center>1/21       
                #2 last  <td nowrap><b>226</b>     
                #3 changetoday  <td nowrap><font color=ff0020>-4</font>
                                #<td nowrap>---
                #4 change_percent  <td nowrap><font color=ff0020>-1.74%</font>
                                   #<td nowrap>
                #5 volume   <td nowrap>1,705,200
                #6 open  <td nowrap>221      
                #7 high  <td nowrap>230
                #8 low   <td nowrap>221

            for line in lines:


                if '§<strong>' in line:
                    local_time = line[line.find(':')-2:line.find(':')+3]
                    local_date = line[line.find('§<strong>')+9:line.find('Æü ')]
                    local_date = local_date.replace('·î ',' ').split()
                    month = local_date[0]
                    if len(month)== 1 : month = '0' + month
                    day = local_date[1]
                    if len(day)== 1 : day = '0' + day
                    local_date = '"'+month+'/'+day+'/'+year+'"'
                    
                if ch in line:
                    n = 1
                if n == 1:
                    i=i+1
                    if i == 1:
                        date_time = line[len(ch):]
                        if date_time.find(':') != -1:
                            #print "last clock"
                            sclock = '"'+date_time+'"'
                            date = local_date
                        else :
                            #print "last date"
                            if date_time == '---':
                                date = 'N/A'
                            else:
                                date = 'N/A'
                                #date = '"'+date_time+'/'+year+'"'
                            sclock = "N/A"
                        #print 'date,hour',date,sclock
                    if i == 2:
                        last = line[line.find('<b>')+3:line.find('</b>')]
                        if last == '---': last = '0.0'
                        last = last.replace(',','')
                        #print 'last:',last
                    if i == 3:
                        if '<td nowrap><font color=ff0020>' in line:
                            change = line[line.find('ff0020>')+7:line.find('</font>')]
                        else:
                            change = line[line.find('<td nowrap>')+11:]
                        if change == '---': change = 'N/A'
                    if i == 4:
                        if '<td nowrap><font color=ff0020>' in line:
                            change_percent = line[line.find('ff0020>')+7:line.find('</font>')]
                        else:
                            change_percent = line[line.find('<td nowrap>')+11:]
                    if i == 5:
                        volume = line[line.find('<td nowrap>')+11:]
                        volume = volume.replace(',','')
                        if volume == '---': volume = '0'

                    if i == 6:
                        open = line[line.find('<td nowrap>')+11:]
                        if open == '---': open = 'N/A'
                        open = open.replace(',','')

                    if i == 7:
                        high = line[line.find('<td nowrap>')+11:]
                        if high == '---': high = 'N/A'
                        high = high.replace(',','')

                    if i == 8:
                        low = line[line.find('<td nowrap>')+11:]
                        if low == '---': low = 'N/A'
                        low = low.replace(',','')

                        n = 0
                        i = 0
                        
                        colors = ['red', 'blue', 'green', 'yellow']
                        result = ''.join(colors)


                        data = ','.join(['"'+ss+'"',last,date,sclock,change,open,high,low,volume])

                        break



            sdata = string.split (data, ',')

            if len (sdata) < 9:
                if itrade_config.verbose:
                    info('invalid data (bad answer length) for %s quote' % (quote.ticker()))
                return None

            # connexion / clock
            self.m_connected = True

            # store for later use
            key = quote.key() 

            sclock = sdata[3][1:-1]
            if sclock=="N/A" or sdata[2]=='"N/A"' or len(sclock)<5:
                if itrade_config.verbose:
                    info('invalid datation for %s : %s : %s' % (quote.ticker(),sclock,sdata[2]))
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
                    info('invalid datation for %s : %s : %s' % (quote.ticker(),sclock,sdata[2]))
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
            #if itrade_config.verbose:
                #print data

            return data
        
        except:
            debug('LiveUpdate_yahoojapan:unable to connect :-(')
            return None

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
        key = quote.key()
        if not self.m_dcmpd.has_key(key):
            # no data for this quote !
            return [],[]
        d = self.m_dcmpd[key]

        buy = []
        #buy.append([0,0,'-'])

        sell = []
        #sell.append([0,0,'-'])

        return buy,sell

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
            if self.m_lastclock==0:
                return "::"
            # hh:mm
            return "%d:%02d" % (self.m_lastclock/60,self.m_lastclock%60)
        #
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
            return "-/-/-"
        else:
            return self.m_dateindice[key]

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
    ignore(gLiveYahoojp)
except NameError:
    gLiveYahoojp = LiveUpdate_yahoojp()

registerLiveConnector('TOKYO EXCHANGE','TKS',QLIST_ANY,QTAG_DIFFERED,gLiveYahoojp,bDefault=True)

# ============================================================================
# Test ME
#
# ============================================================================

def test(ticker):
    if gLiveYahoojp.iscacheddataenoughfreshq():
        data = gLiveYahoojp.getcacheddata(ticker)
        if data:
            debug(data)
        else:
            debug("nodata")

    elif gLiveYahoojp.connect():

        state = gLiveYahoojp.getstate()
        if state:
            debug("state=%s" % (state))

            quote = quotes.lookupTicker(ticker,'TOKYO EXCHANGE')
            data = gLiveYahoojp.getdata(Quote)
            if data!=None:
                if data:
                    info(data)
                else:
                    debug("nodata")
            else:
                print "getdata() failure :-("
        else:
            print "getstate() failure :-("

        gLiveYahoojp.disconnect()
    else:
        print "connect() failure :-("

if __name__=='__main__':
    setLevel(logging.INFO)

    print 'live %s' % date.today()
    test('AAPL')

# ============================================================================
# That's all folks !
# ============================================================================
