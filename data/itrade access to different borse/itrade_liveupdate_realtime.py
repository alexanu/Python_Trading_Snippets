#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_liveupdate_realtime.py(boursorama)
#
# Description: Live update quotes from abcbourse.com
#
# The Original Code is iTrade code (http://itrade.sourceforge.net).
#
# The Initial Developer of the Original Code is	Gilles Dumortier.
# New code for realtime is from Jean-Marie Pacquet and Michel Legrand.

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
# 2005-03-25    dgil  Wrote it from scratch
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
import urllib2
import cPickle

# iTrade system
import itrade_config
from itrade_logging import *
from itrade_quotes import *
from itrade_defs import *
from itrade_ext import *
from itrade_market import yahooTicker,convertConnectorTimeToPlaceTime
from itrade_connection import ITradeConnection
from datetime import *

# ============================================================================
# LiveUpdate_RealTime()
#
# ============================================================================

class LiveUpdate_RealTime(object):
    def __init__(self,market = 'EURONEXT'):
        debug('LiveUpdate_RealTime:__init__')
        self.m_connected = False
        self.m_livelock = thread.allocate_lock()        
        self.m_conn = None
        self.m_clock = {}
        self.m_dateindice = {}
        self.m_dcmpd = {}
        self.m_lastclock = 0
        self.m_lastdate = "20070101"
        self.m_market = market
        self.m_connection = ITradeConnection(cookies = None,
                                           proxy = itrade_config.proxyHostname,
                                           proxyAuth = itrade_config.proxyAuthentication,
                                           connectionTimeout = itrade_config.connectionTimeout
                                           )
        
        try:
            select_isin = []
            self.m_isinsymbol = {}
            symbol = ''
        
            # try to open dictionnary of ticker_bourso.txt
            f = open(os.path.join(itrade_config.dirUserData,'ticker_bourso.txt'),'r')
            self.m_isinsymbol = cPickle.load(f)
            f.close()

        except:
            print 'Missing or invalid file: ticker_bourso.txt'

            # read isin codes of properties.txt file in directory usrdata
            try:
                source = open(os.path.join(itrade_config.dirUserData,'properties.txt'),'r')
                data = source.readlines()
                source.close()
                for linedata in data:
                    if 'live;realtime' in linedata:
                        isin = linedata[:linedata.find('.')]
                        debug('isin:%s' % isin)
                        select_isin.append(isin)
                        debug('%s' % select_isin)

                # extract pre_symbol
                for isin in select_isin:
                    req = urllib2.Request('http://www.boursorama.com/recherche/index.phtml?search%5Bquery%5D=' + isin)
                    req.add_header('User-Agent', 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.7.5) Gecko/20041202 Firefox/1.0')
                    try:
                        f = urllib2.urlopen(req)
                        data = f.read()
                        f.close()

                        ch = 'class="bourse fit block" >'

                        if data.find(ch)!= -1:
                            b = data.find(ch)
                            if data.find('>Valeurs<',b) != - 1:
                                if data.find('class="exchange">Nyse Euro<',b) != -1:
                                    c = data.find('class="exchange">Nyse Euro<',b)
                                    a = data.rfind('href="/cours.phtml?symbole=',0,c)
                                    symbol = data[a+27:a+43]
                                    symbol = symbol[:symbol.find('" >')]
                                    self.m_isinsymbol [isin] = symbol
                                    debug('%s found and added in dictionary (%s)' % (isin,symbol))
                    except:
                        pass

                dic = open(os.path.join(itrade_config.dirUserData,'ticker_bourso.txt'), 'w')
                cPickle.dump(self.m_isinsymbol,dic)
                dic.close()

            except:
                pass

    # ---[ reentrant ] ---
    def acquire(self):
        self.m_livelock.acquire()

    def release(self):
        self.m_livelock.release()

    # ---[ properties ] ---

    def name(self):
        return 'realtime'

    def delay(self):
        return 0

    def timezone(self):
        # timezone of the livedata (see pytz all_timezones)
        return "Europe/Paris"

    # ---[ connexion ] ---

    def connect(self):
        return True

    def disconnect(self):
        #pass
        if self.m_conn:
            self.m_conn.close()
        self.m_conn = None
        self.m_connected = False
    
    def alive(self):
        return self.m_connected

    # ---[ state ] ---

    def getstate(self):
        # no state
        return True

    # ---[ code to get data ] ---
    
    def splitLines(self,data):
        lines = string.split(data, '\n')
        lines = filter(lambda x:x, lines)
        def removeCarriage(s):
            if s[-1]=='\r':
                return s[:-1]
            else:
                return s
        lines = [removeCarriage(l) for l in lines]
        return lines

    def BoursoDate(self,date):
        sp = string.split(date,' ')

        # Date part is easy
        sdate = jjmmaa2yyyymmdd(sp[0])

        if len(sp)==1:
            return sdate,"00:00"
        return sdate,sp[1]
    
    
   
    def convertClock(self,place,clock,date):
        min = clock[3:5]
        hour = clock[:2]
        val = (int(hour)*60) + int(min)

        if val>self.m_lastclock and date>=self.m_lastdate:
            self.m_lastdate = date
            self.m_lastclock = val

        # convert from connector timezone to market place timezone
        mdatetime = datetime(int(date[0:4]),int(date[4:6]),int(date[6:8]),val/60,val%60)
        mdatetime = convertConnectorTimeToPlaceTime(mdatetime,self.timezone(),place)

        return "%d:%02d" % (mdatetime.hour,mdatetime.minute)
    

    
    def getdata(self,quote):
        self.m_connected = False
        debug("LiveUpdate_Bousorama:getdata quote:%s market:%s" % (quote,self.m_market))

        isin = quote.isin()

        # add a value, default is yahoo connector
        # with boursorama realtime connector, must have pre_symbol to extract quote

        if isin != '' :
            if  not isin in self.m_isinsymbol:

                req = urllib2.Request('http://www.boursorama.com/recherche/index.phtml?search%5Bquery%5D=' + isin)
                req.add_header('User-Agent', 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.7.5) Gecko/20041202 Firefox/1.0')
                
                try: 
                    f = urllib2.urlopen(req)
                    data = f.read()
                    f.close()
                        
                    ch = 'class="bourse fit block" >'

                    if data.find(ch)!= -1:
                        b = data.find(ch)
                        if data.find('>Valeurs<',b) != - 1:
                            if data.find('class="exchange">Nyse Euro<',b) != -1:
                                c = data.find('class="exchange">Nyse Euro<',b)
                                a = data.rfind('href="/cours.phtml?symbole=',0,c)
                                symbol = data[a+27:a+43]
                                symbol = symbol[:symbol.find('" >')]
                                self.m_isinsymbol [isin] = symbol
                                debug('%s found and added in dictionary (%s)' % (isin,symbol))
                                dic = open(os.path.join(itrade_config.dirUserData,'ticker_bourso.txt'), 'w')
                                cPickle.dump(self.m_isinsymbol,dic)
                                dic.close()
                            else:
                                return None
                        else:
                            return None
                    else:
                        return None

                except:
                    debug('LiveUpdate_Boursorama:unable to connect :-(')
                    return None
        else:
            return None

        symbol = self.m_isinsymbol[isin]
        debug('Symbole=%s' % symbol)

        # extract all datas

        try:
            if ('1rT' in symbol or
               '1RT' in symbol or
               '1z' in symbol or
               '1g' in symbol):
                req = urllib2.Request('http://www.boursorama.com/bourse/trackers/etf.phtml?symbole=' + symbol)
            else:
                req = urllib2.Request('http://www.boursorama.com/cours.phtml?symbole=' + symbol)
            req.add_header('User-Agent', 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.7.5) Gecko/20041202 Firefox/1.0')

            f = urllib2.urlopen(req)
            data = f.read()
            f.close()
        except:
            debug('LiveUpdate_Boursorama:unable to connect :-(')
            return None

        data = data.replace('\t','').replace ('</span>','')
        lines = self.splitLines(data)
        n = -1
        for line in lines:
            n=n+1
            if '<table class="info-valeur list">' in line:
                line = lines[n+6]
                value = line[line.find('"cotation">')+11:line.find('</b>')]
                if '(' in value:
                    stat = value[value.find('(')+1:value.find(')')]
                else :
                    stat = ''
                if ('USD' in value or
                   'GBX' in value or
                   'GBP' in value or
                   'CAD' in value or
                   'CHF' in value):
                    pass
                else:
                    last = value.replace(' ','').replace('EUR','').replace('Pts','').replace('(s)','').replace('(c)','').replace('(h)','').replace('(u)','')
                    last = last.replace('%','')
                    line = lines[n+11]
                    percent = line[line.rfind('">')+2:line.find('%</td>')].replace(' ','')

                    line = lines[n+15]
                    date_time = line[line.find('<td>')+4:line.find('</td>')]
                    date_time = date_time[:8]+' '+date_time[-8:]

                    line = lines[n+19]
                    volume = line[line.rfind('">')+2:line.find('</td>')].replace(' ','').replace('<td>','').replace('td>','')
                    if 'M' in line : volume  = '0'
                    if volume == '0' and quote.list()!=QLIST_INDICES:
                        #info('volume : no trade to day %s' % symbol)
                        return None
                    line = lines[n+23]
                    first = line[line.find('"cotation">')+11:line.find('</td>')].replace(' ','')

                    line = lines[n+27]
                    high = (line[line.find('"cotation">')+11:line.find('</td>')]).replace(' ','')

                    line = lines[n+31]
                    low = line[line.find('"cotation">')+11:line.find('</td>')].replace(' ','')

                    line = lines[n+35]
                    previous = line[line.find('"cotation">')+11:line.find('</td>')].replace(' ','')

                    c_datetime = datetime.today()
                    c_date = "%04d%02d%02d" % (c_datetime.year,c_datetime.month,c_datetime.day)

                    sdate,sclock = self.BoursoDate(date_time)

                    # be sure not an oldest day !
                    if (c_date==sdate) or (quote.list() == QLIST_INDICES):
                        key = quote.key()
                        self.m_dcmpd[key] = sdate
                        self.m_dateindice[key] = str(sdate[6:8]) + '/' + str(sdate[4:6]) + '/' +str(sdate[0:4])
                        self.m_clock[key] = self.convertClock(quote.place(),sclock,sdate)

                    data = ';'.join([quote.key(),sdate,first,high,low,last,volume,percent])
                    #print "connect to Boursorama",quote.key()
                    return data

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
        return False

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
            return "----"
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

if __name__=='__main__':
    setLevel(logging.DEBUG)
gLiveRealTime = LiveUpdate_RealTime()
gLiveAlternext = LiveUpdate_RealTime()

registerLiveConnector('EURONEXT','PAR',QLIST_ANY,QTAG_LIVE,gLiveRealTime,bDefault=False)
#registerLiveConnector('EURONEXT','BRU',QLIST_ANY,QTAG_LIVE,gLiveRealTime,bDefault=False)
#registerLiveConnector('EURONEXT','AMS',QLIST_ANY,QTAG_LIVE,gLiveRealTime,bDefault=False)
#registerLiveConnector('EURONEXT','LIS',QLIST_ANY,QTAG_LIVE,gLiveRealTime,bDefault=False)
registerLiveConnector('EURONEXT','PAR',QLIST_INDICES,QTAG_LIVE,gLiveRealTime,bDefault=False)
registerLiveConnector('EURONEXT','BRU',QLIST_INDICES,QTAG_LIVE,gLiveRealTime,bDefault=False)
registerLiveConnector('EURONEXT','AMS',QLIST_INDICES,QTAG_LIVE,gLiveRealTime,bDefault=False)
registerLiveConnector('EURONEXT','LIS',QLIST_INDICES,QTAG_LIVE,gLiveRealTime,bDefault=False)
registerLiveConnector('ALTERNEXT','PAR',QLIST_ANY,QTAG_LIVE,gLiveRealTime,bDefault=False)

#registerLiveConnector('ALTERNEXT','AMS',QLIST_ANY,QTAG_LIVE,gLiveRealTime,bDefault=False)
#registerLiveConnector('ALTERNEXT','BRU',QLIST_ANY,QTAG_LIVE,gLiveRealTime,bDefault=False)
#registerLiveConnector('ALTERNEXT','LIS',QLIST_ANY,QTAG_LIVE,gLiveRealTime,bDefault=False)

registerLiveConnector('PARIS MARCHE LIBRE','PAR',QLIST_ANY,QTAG_LIVE,gLiveRealTime,bDefault=False)
#registerLiveConnector('BRUXELLES MARCHE LIBRE','BRU',QLIST_ANY,QTAG_LIVE,gLiveRealTime,bDefault=False)

# ============================================================================
# Test ME
#
# ============================================================================

def test(ticker):
    if gLiveRealTime.iscacheddataenoughfreshq():
        data = gLiveRealTime.getcacheddata(ticker)
        if data:
            debug(data)
        else:
            debug("nodata")

    elif gLiveRealTime.connect():

        state = gLiveRealTime.getstate()
        if state:
            debug("state=%s" % (state))

            quote = quotes.lookupTicker(ticker,'EURONEXT')
            if (quote):
                data = gLiveRealTime.getdata(quote)
                if data!=None:
                    if data:
                        info(data)
                    else:
                        debug("nodata")
                else:
                    print "getdata() failure :-("
            else:
                print "Unknown ticker %s on EURONEXT" % (ticker)
        else:
            print "getstate() failure :-("

        gLiveRealTime.disconnect()
    else:
        print "connect() failure :-("

if __name__=='__main__':
    print 'live %s' % date.today()
   # load euronext import extension
    import itrade_ext
    itrade_ext.loadOneExtension('itrade_import_euronext.py',itrade_config.dirExtData)
    quotes.loadMarket('EURONEXT')

    test('OSI')
    test('GTO')
    gLiveRealTime.cacheddatanotfresh()
    test('GTO')

# ============================================================================
# That's all folks !
# ============================================================================
