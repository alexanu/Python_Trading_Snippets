#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_liveupdate_fortuneo.py
#
# Description: Live update quotes from fortuneo
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
# 2005-12-29    dgil  Wrote it from scratch
# 2006-05-0x    dgil  live.txt
# 2006-05-28    dgil  deprecated - live broken / access has been securized
#                     keep it for historical reason
#
# 2006-08-17    dgil  working on the new mechanism ...
# 2006-08-19    dgil  authentication is working !!!!!!
# ============================================================================

# ============================================================================
# Imports
# ============================================================================

# python system
import logging
import re
import string
import thread
import datetime
import os
import socket
import httplib
import datetime

# iTrade system
import itrade_config
from itrade_logging import *
from itrade_quotes import *
from itrade_defs import *
from itrade_ext import *

import blowfish

# ============================================================================
# Flux to Place
# ============================================================================

flux_place = {
    "025" : "FRANCE.PL",
    "027" : "FRANCE.PL",
    "028" : "FRANCE.PL",
    "029" : "FRANCE.PL",
    "030" : "FRANCE.PL",
    "031" : "FRANCE.PL",
    "032" : "FRANCE.PL",
    "033" : "FRANCE.PL",
    "260" : "FRANCE.PL",
    "485" : "FRANCE.PL",

    "004" : "EUROPE1.PL",
    "011" : "EUROPE1.PL",
    "013" : "EUROPE1.PL",
    "014" : "EUROPE1.PL",
    "015" : "EUROPE1.PL",
    "016" : "EUROPE1.PL",
    "017" : "EUROPE1.PL",
    "018" : "EUROPE1.PL",
    "019" : "EUROPE1.PL",
    "022" : "EUROPE1.PL",
    "038" : "EUROPE1.PL",
    "046" : "EUROPE1.PL",
    "220" : "EUROPE1.PL",

    "036" : "EUROPE2.PL",
    "232" : "EUROPE2.PL",
    "361" : "EUROPE2.PL",
    "613" : "EUROPE2.PL",
    "615" : "EUROPE2.PL",

    "055" : "ESPAGNE.PL",
    "056" : "ESPAGNE.PL",
    "057" : "ESPAGNE.PL",
    "058" : "ESPAGNE.PL",
    "355" : "ESPAGNE.PL",

    "067" : "AMERIQUE1.PL",
    "130" : "AMERIQUE1.PL",

    "065" : "AMERIQUE2.PL",
    "066" : "AMERIQUE2.PL",
    "145" : "AMERIQUE2.PL",

    "012" : "RESTEDUMONDE.PL",
    "040" : "RESTEDUMONDE.PL",
    "048" : "RESTEDUMONDE.PL",
    "050" : "RESTEDUMONDE.PL",
    "051" : "RESTEDUMONDE.PL",
    "053" : "RESTEDUMONDE.PL",
    "061" : "RESTEDUMONDE.PL",
    "072" : "RESTEDUMONDE.PL",
    "083" : "RESTEDUMONDE.PL",
    "103" : "RESTEDUMONDE.PL",
    "104" : "RESTEDUMONDE.PL",
    "106" : "RESTEDUMONDE.PL",
    "111" : "RESTEDUMONDE.PL",
    "120" : "RESTEDUMONDE.PL",
    "152" : "RESTEDUMONDE.PL",
    "241" : "RESTEDUMONDE.PL",
    "244" : "RESTEDUMONDE.PL",
    "249" : "RESTEDUMONDE.PL",
    "267" : "RESTEDUMONDE.PL",
    "373" : "RESTEDUMONDE.PL",
    "428" : "RESTEDUMONDE.PL",
    "498" : "RESTEDUMONDE.PL"
}

def flux2place(flux):
    if flux_place.has_key(flux):
        return flux_place[flux]
    else:
        # default
        return 'FRANCE.PL'

# ============================================================================
# subscriptions
#
# FR0003500008 : CAC40 indice
# ============================================================================

full_subscriptions = (
    "CSA_CRS_DERNIER",
    "CSA_VAR_VEILLE",
    "CSA_CRS_PREMIER",
    "CSA_CRS_HAUT",
    "CSA_CRS_BAS",
    "CSA_VOL_JOUR",

    "CSA_NBL_DEM1",
    "CSA_VOL_DEM1",
    "CSA_CRS_DEM1",
    "CSA_CRS_OFF1",
    "CSA_VOL_OFF1",
    "CSA_NBL_OFF1",

    "CSA_NBL_DEM2",
    "CSA_VOL_DEM2",
    "CSA_CRS_DEM2",
    "CSA_CRS_OFF2",
    "CSA_VOL_OFF2",
    "CSA_NBL_OFF2",

    "CSA_NBL_DEM3",
    "CSA_VOL_DEM3",
    "CSA_CRS_DEM3",
    "CSA_CRS_OFF3",
    "CSA_VOL_OFF3",
    "CSA_NBL_OFF3",

    "CSA_NBL_DEM4",
    "CSA_VOL_DEM4",
    "CSA_CRS_DEM4",
    "CSA_CRS_OFF4",
    "CSA_VOL_OFF4",
    "CSA_NBL_OFF4",

    "CSA_NBL_DEM5",
    "CSA_VOL_DEM5",
    "CSA_CRS_DEM5",
    "CSA_CRS_OFF5",
    "CSA_VOL_OFF5",
    "CSA_NBL_OFF5",

    "CSA_FMP_DEM",
    "CSA_FMP_OFF",

    "CSA_HD_COURS",
    "CSA_VOL_DERNIER",
    "CSA_H_TRANS_2",
    "CSA_VOL_TRANS_2",
    "CSA_CRS_TRANS_2",
    "CSA_H_TRANS_3",
    "CSA_VOL_TRANS_3",
    "CSA_CRS_TRANS_3",
    "CSA_H_TRANS_4",
    "CSA_VOL_TRANS_4",
    "CSA_CRS_TRANS_4",
    "CSA_H_TRANS_5",
    "CSA_VOL_TRANS_5",
    "CSA_CRS_TRANS_5",

    "CSA_CRS_CMP",
    "CSA_IND_ETAT",
    "CSA_H_REPRIS_COT",
    "CSA_RESERV_HAUT",
    "CSA_RESERV_BAS"
    )

def isin2sub(isin,sub,flux):
    return "%s%s.%s.%s" % (flux2place(flux),flux,isin.strip().upper(),sub.strip().upper())

def isin2subscriptions(isin,flux):
    str = ""
    for each in full_subscriptions:
        if str!="":
            str = str+","
        str = str + isin2sub(isin,each,flux)
    return str

indice_subscriptions = (
    "CSA_CRS_DERNIER",
    "CSA_HD_COURS",
    "CSA_VAR_VEILLE"
    )

def indice2subscriptions(isin,flux):
    str = ""
    for each in indice_subscriptions:
        if str!="":
            str = str+","
        str = str + isin2sub(isin,each,flux)
    return str

index2field = {
    '000' : 0,
    '001' : 1,
    '002' : 2,
    '003' : 3,
    '004' : 4,
    '005' : 5,
    '006' : 6,
    '007' : 7,
    '008' : 8,
    '009' : 9,
    '00A' : 10,
    '00B' : 11,
    '00C' : 12,
    '00D' : 13,
    '00E' : 14,
    '00F' : 15,
    '00G' : 16,
    '00H' : 17,
    '00I' : 18,
    '00J' : 19,
    '00K' : 20,
    '00L' : 21,
    '00M' : 22,
    '00N' : 23,
    '00O' : 24,
    '00P' : 25,
    '00Q' : 26,
    '00R' : 27,
    '00S' : 28,
    '00T' : 29,
    '00U' : 30,
    '00V' : 31,
    '00W' : 32,
    '00X' : 33,
    '00Y' : 34,
    '00Z' : 35,
    '010' : 36,
    '011' : 37,
    '012' : 38,
    '013' : 39,
    '014' : 40,
    '015' : 41,
    '016' : 42,
    '017' : 43,
    '018' : 44,
    '019' : 45,
    '01A' : 46,
    '01B' : 47,
    '01C' : 48,
    '01D' : 49,
    '01E' : 50,
    '01F' : 51,
    '01G' : 52,
    '01H' : 53,
    '01I' : 54,
    '01J' : 55,
    '01K' : 56,     # last of quote
    '01L' : 57,
    '01M' : 58,
    '01N' : 59

}

# ============================================================================
# place -> code place
# ============================================================================

place_code = {
    "004" : "004",
    "SUISSE" : "004",

    "006" : "006",
    "BRUXELLES" : "006",

    "025" : "025",
    "PARIS": "025",

    "027" : "027",
    "LYON" : "027",

    "028" : "028",
    "MARSEILLE" : "028",

    "029" : "029",
    "NANCY" : "029",

    "030" : "030",
    "LYON" : "030",

    "031" : "031",
    "NANTES" : "031",

    "032" : "032",
    "LILLE" : "032",

    "036" : "036",
    "361" : "361",
    "LONDON" : "036",

    "038" : "038",
    "AMSTERDAM" : "038",

    "044" : "044",
    "863" : "863",
    "XETRA" : "044",

    "046" : "046",
    "ITALIE" : "046",

    "054" : "054",
    "ESPAGNE" : "054",

    "065" : "065",
    "NYSE" : "065",

    "066" : "066",
    "ASE" : "066",
    "NYSE" : "066",

    "067" : "067",
    "NASDAQ" : "067",

    "260" : "260",
    "OPCVM" : "260",

    "046" : "046",
    "220" : "220",

}

def place2code(place):
    if place_code.has_key(place):
        return place_code[place]
    else:
        # default : PARIS
        return '025'

# ============================================================================
#
# ============================================================================

def convert(n,v,s):
    n = int(n)
    v = int(v)
    if s=='-2':
        s = 'ATP'
    if s=='0' or s=='0.00':
        if v>0:
            s = 'APM'
        else:
            s = '-'
    return n,v,s

# ============================================================================
# encode_topics
# ============================================================================

h2n = {
    '0': 0,
    '1': 1,
    '2': 2,
    '3': 3,
    '4': 4,
    '5': 5,
    '6': 6,
    '7': 7,
    '8': 8,
    '9': 9,
    'A': 10,
    'B': 11,
    'C': 12,
    'D': 13,
    'E': 14,
    'F': 15
    }

def hex2num(hex):
    d = h2n[hex[0]]*16 + h2n[hex[1]]
    if d>127:
        d = d - 256
    return d

def encode_topic(topic,key):
    blowfish.initialize(key)

    instring = topic
    blocks = len(instring)/8
    padding = (blocks + 1)*8 - len(instring)
    instring = instring + padding * chr(padding)
    print "Padding with %s bytes" % padding

    outstring = ""

    print "Encrypting..."
    for i in range(blocks+1):
        inbytes   = blowfish.mkchunk(instring[i*8:i*8+8])   # 8-byte string as hex no.
        cipher    = blowfish.bfencrypt(inbytes)             # ...encrypted
        outbytes  = "%016X" % cipher
        outstring = outstring + outbytes                    # and appended

    topic = ""
    print "Size: %s / %s" % (len(instring),len(outstring))
    print '"%s"' % outstring
    for i in range(0,len(outstring),2):
        if i>0: topic = topic + "%3B"
        topic = topic + '%d' % hex2num(outstring[i:i+2])

    return '%5B'+topic+'%5D'

def encode_topics(topics,key):
    key = blowfish.mkchunk(key)
    topics = topics.split(',')
    ret = ""

    j=0
    for topic in topics:
        if j>0:
            ret = ret + '%2C'
        ret = ret + encode_topic(topic.encode("ISO-8859-1"),key)
        j = j + 1

    return ret

# ============================================================================
# LiveUpdate_fortuneo()
#
# ============================================================================

class LiveUpdate_fortuneo(object):
    def __init__(self):
        debug('LiveUpdate_fortuneo:__init__')
        self.m_default_host = "streaming.fortuneo.fr"
        #self.m_default_host = "81.255.56.22"
        self.m_conn = None
        self.m_connected = False

        self.m_livelock = thread.allocate_lock()
        self.m_dcmpd = {}
        self.m_clock = {}
        self.m_lastclock = 0

        self.m_cookie = None
        self.m_blowfish = None
        self.m_places = None

    # ---[ read cookie ] ---
    def readCookie(self):
        if self.m_cookie==None:
            try:
                f = open(os.path.join(itrade_config.dirUserData,'fortuneo_live.txt'),'r')
                infile = f.readlines()
                txt = infile[0].strip()
                f.close()
                self.m_cookie,self.m_blowfish = txt.split('-')
                print 'cookie,blowfish:',self.m_cookie,self.m_blowfish
            except IOError:
                self.m_cookie = ''

    # ---[ reentrant ] ---
    def acquire(self):
        self.m_livelock.acquire()

    def release(self):
        self.m_livelock.release()

    # ---[ properties ] ---

    def name(self):
        return 'fortuneo'

    def delay(self):
        return 0

    def timezone(self):
        # timezone of the livedata (see pytz all_timezones)
        return "Europe/Paris"

    # ---[ connexion ] ---

    def connect(self):
        self.m_conn = httplib.HTTPConnection(self.m_default_host,80)
        if self.m_conn == None:
            print 'live: not connected on %s:80' % self.m_default_host
            return False

        self.readCookie()
        self.loadPlaces()

        #if itrade_config.verbose:
        #    print 'live: connected on %s:80' % self.m_default_host
        return True

    def disconnect(self):
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

    # ---[ specific code to manage place ] ---

    def loadPlaces(self):
        if self.m_places == None:
            self.m_places = {}
            infile = itrade_csv.read(None,os.path.join(itrade_config.dirSysData,'places.txt'))
            if infile:
                # scan each line to read each quote
                for eachLine in infile:
                    item = itrade_csv.parse(eachLine,2)
                    if item:
                        self.m_places[item[0]] = place2code(item[1].strip().upper())

    def place(self,isin):
        if self.m_places.has_key(isin) : return self.m_places[isin]
        return "025"

    # ---[ code to get data ] ---

    def convertClock(self,clock):
        clo = clock[:-3]
        min = clo[-2:]
        hour = clo[:-3]
        val = (int(hour)*60) + int(min)
        #print clo,hour,min,val
        if val>self.m_lastclock:
            #print "lastclock was :",self.m_lastclock," then is : ",val
            self.m_lastclock = val
        return "%d:%02d" % (val/60,val%60)

    def getdata(self,quote):
        # check we have a connection
        if not self.m_conn:
            raise('LiveUpdate_fortuneo:no connection / missing connect() call !')
            return None

        #info("LiveUpdate_fortuneo:getdata quote:%s " % quote)

        isin = quote.isin()

        self.m_connected = False

        # init params and headers
        headers = {
                    "Connection":"keep-alive",
                    "Accept":"text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2",
                    "Host":self.m_default_host,
                    "User-Agent":"Mozilla/4.0 (Windows XP 5.1) Java/1.5.0_06",
                    "Pragma":"no-cache",
                    "Cache-Control":"no-cache",
                    "Content-Type":"application/x-www-form-urlencoded"
                    }

        cac = "FR0003500008"
        #topics = "%s,%s" % (isin2subscriptions(isin,self.place(isin)),indice2subscriptions(cac,self.place(cac)))
        topics = indice2subscriptions(cac,self.place(cac))
        topics = encode_topics(topics,self.m_blowfish)

        params = "subscriptions%%3D%%7B%s%%7D%%26userinfo%%3D%s\r\n" % (topics,self.m_cookie)
        print 'params:',params

        # POST quote request
        try:
            self.m_conn.request("POST", "/streaming", params, headers)
            flux = self.m_conn.getresponse()
        except:
            info('LiveUpdate_fortuneo:POST failure')
            return None

        if flux.status != 200:
            info('LiveUpdate_fortuneo: status==%d!=200 reason:%s headers:%s' % (flux.status,flux.reason,flux.getheaders()))
            return None

        self.m_connected = True

        # init some defaut values (just in case not in the server answer ...)
        dcmpd = {}

        dcmpd['CSA_H_TRANS_2'] = '0:00:00'
        dcmpd['CSA_H_TRANS_3'] = '0:00:00'
        dcmpd['CSA_H_TRANS_4'] = '0:00:00'
        dcmpd['CSA_H_TRANS_5'] = '0:00:00'

        dcmpd['CSA_NBL_DEM1'] = '0'
        dcmpd['CSA_NBL_DEM2'] = '0'
        dcmpd['CSA_NBL_DEM3'] = '0'
        dcmpd['CSA_NBL_DEM4'] = '0'
        dcmpd['CSA_NBL_DEM5'] = '0'
        dcmpd['CSA_CRS_DEM1'] = '0.00'
        dcmpd['CSA_CRS_DEM2'] = '0.00'
        dcmpd['CSA_CRS_DEM3'] = '0.00'
        dcmpd['CSA_CRS_DEM4'] = '0.00'
        dcmpd['CSA_CRS_DEM5'] = '0.00'

        dcmpd['CSA_NBL_OFF1'] = '0'
        dcmpd['CSA_NBL_OFF2'] = '0'
        dcmpd['CSA_NBL_OFF3'] = '0'
        dcmpd['CSA_NBL_OFF4'] = '0'
        dcmpd['CSA_NBL_OFF5'] = '0'
        dcmpd['CSA_CRS_OFF1'] = '0.00'
        dcmpd['CSA_CRS_OFF2'] = '0.00'
        dcmpd['CSA_CRS_OFF3'] = '0.00'
        dcmpd['CSA_CRS_OFF4'] = '0.00'
        dcmpd['CSA_CRS_OFF5'] = '0.00'

        dcmpd['CSA_H_REPRIS_COT'] = ''
        dcmpd['CSA_IND_ETAT'] = ''
        dcmpd['CSA_FMP_DEM'] = '0.00'
        dcmpd['CSA_FMP_OFF'] = '0.00'
        dcmpd['CSA_CRS_CMP'] = '0.00'

        dcmpd['CSA_VOL_JOUR'] = '0'
        dcmpd['CSA_VOL_DERNIER'] = '0'
        dcmpd['CSA_CRS_DERNIER'] = '0.00'

        # read the streaming flux
        while 1:
            # get index
            index = flux.read(3)

            # get value
            value = ''
            while 1 :
                data = flux.read(1)
                if data=='\n':
                    break
                else:
                    value = value + data

            # get field
            numind = index2field[index]
            if numind <= 56:
                field = full_subscriptions[numind]

                # store information
                # print '%s: %s = %s' % (index,field,value)
                dcmpd[field] = value
            else:
                field = indice_subscriptions[numind-57]

                # store information
                # print '%s: %s = %s' % (index,field,value)
                # indice[field] = value

            if index == '01N':
                break

        # close the stream
        flux.close()

        # usefull information ?
        if not dcmpd.has_key('CSA_IND_ETAT'):
            info("LiveUpdate_fortuneo:getdata quote:%s UNKNOWN QUOTE? or WRONG PLACE?" % quote)
            return None

        # store in cache
        self.m_dcmpd[isin] = dcmpd

        # extrack date
        if not dcmpd.has_key('CSA_HD_COURS') or not dcmpd.has_key('CSA_CRS_PREMIER'):
            info("LiveUpdate_fortuneo:getdata quote:%s CLOSED" % quote)
            return None

        cl = dcmpd['CSA_HD_COURS']
        dt = cl[:8]
        dt = '20' + cl[6:8] + '-' + cl[3:5] + '-' + cl[0:2]

        # extract clock
        self.m_clock[isin] = self.convertClock(cl[8:])
        #print 'clock:',self.m_clock[isin]

        # ISIN;DATE;OPEN;HIGH;LOW;CLOSE;VOLUME
        data = (
          quote.key(),
          dt,
          dcmpd['CSA_CRS_PREMIER'],
          dcmpd['CSA_CRS_HAUT'],
          dcmpd['CSA_CRS_BAS'],
          dcmpd['CSA_CRS_DERNIER'],
          dcmpd['CSA_VOL_JOUR']
        )
        data = map(lambda (val): '%s' % str(val), data)
        data = string.join(data, ';')
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
        isin = quote.isin()
        if not self.m_dcmpd.has_key(isin):
            # no data for this quote !
            return [],[]
        d = self.m_dcmpd[isin]

        #
        buy = []
        if d['CSA_NBL_DEM1'] != "0":
            buy.append(convert(d['CSA_NBL_DEM1'],d['CSA_VOL_DEM1'],d['CSA_CRS_DEM1']))
            if d['CSA_NBL_DEM2'] != "0":
                buy.append(convert(d['CSA_NBL_DEM2'],d['CSA_VOL_DEM2'],d['CSA_CRS_DEM2']))
                if d['CSA_NBL_DEM3'] != "0":
                    buy.append(convert(d['CSA_NBL_DEM3'],d['CSA_VOL_DEM3'],d['CSA_CRS_DEM3']))
                    if d['CSA_NBL_DEM4'] != "0":
                        buy.append(convert(d['CSA_NBL_DEM4'],d['CSA_VOL_DEM4'],d['CSA_CRS_DEM4']))
                        if d['CSA_NBL_DEM5'] != "0":
                            buy.append(convert(d['CSA_NBL_DEM5'],d['CSA_VOL_DEM5'],d['CSA_CRS_DEM5']))

        sell = []
        if d['CSA_NBL_OFF1'] != "0":
            sell.append(convert(d['CSA_NBL_OFF1'],d['CSA_VOL_OFF1'],d['CSA_CRS_OFF1']))
            if d['CSA_NBL_OFF2'] != "0":
                sell.append(convert(d['CSA_NBL_OFF2'],d['CSA_VOL_OFF2'],d['CSA_CRS_OFF2']))
                if d['CSA_NBL_OFF3'] != "0":
                    sell.append(convert(d['CSA_NBL_OFF3'],d['CSA_VOL_OFF3'],d['CSA_CRS_OFF3']))
                    if d['CSA_NBL_OFF4'] != "0":
                        sell.append(convert(d['CSA_NBL_OFF4'],d['CSA_VOL_OFF4'],d['CSA_CRS_OFF4']))
                        if d['CSA_NBL_OFF5'] != "0":
                            sell.append(convert(d['CSA_NBL_OFF5'],d['CSA_VOL_OFF5'],d['CSA_CRS_OFF5']))

        return buy,sell

    def currentClock(self,quote=None):
        if quote==None:
            if self.m_lastclock==0:
                return "::"
            # hh:mm
            return "%d:%02d" % (self.m_lastclock/60,self.m_lastclock%60)
        #
        isin = quote.isin()
        if not self.m_clock.has_key(isin):
            # no data for this quote !
            return "::"
        else:
            return self.m_clock[isin]

    def currentTrades(self,quote):
        #
        isin = quote.isin()
        if not self.m_dcmpd.has_key(isin):
            # no data for this quote !
            return None
        d = self.m_dcmpd[isin]

        # clock,volume,value
        if not d.has_key('CSA_HD_COURS'):
            return None

        last = []
        cl = d['CSA_HD_COURS'][8:]
        if cl != "0:00:00":
            last.append((cl,int(d['CSA_VOL_DERNIER']),d['CSA_CRS_DERNIER']))
            if d['CSA_H_TRANS_2'] != "0:00:00":
                last.append((d['CSA_H_TRANS_2'],int(d['CSA_VOL_TRANS_2']),d['CSA_CRS_TRANS_2']))
                if d['CSA_H_TRANS_3'] != "0:00:00":
                    last.append((d['CSA_H_TRANS_3'],int(d['CSA_VOL_TRANS_3']),d['CSA_CRS_TRANS_3']))
                    if d['CSA_H_TRANS_4'] != "0:00:00":
                        last.append((d['CSA_H_TRANS_4'],int(d['CSA_VOL_TRANS_4']),d['CSA_CRS_TRANS_4']))
                        if d['CSA_H_TRANS_5'] != "0:00:00":
                            last.append((d['CSA_H_TRANS_5'],int(d['CSA_VOL_TRANS_5']),d['CSA_CRS_TRANS_5']))

        return last

    def currentMeans(self,quote):
        #
        isin = quote.isin()
        if not self.m_dcmpd.has_key(isin):
            # no data for this quote !
            return "-","-","-"
        d = self.m_dcmpd[isin]

        s = d['CSA_FMP_DEM']
        if s=='0.00':
            s = '-'

        b = d['CSA_FMP_OFF']
        if b=='0.00':
            b = '-'

        tcmp = float(d['CSA_CRS_CMP'])
        if tcmp<=0.0:
            tcmp = '-'
        else:
            tcmp = "%.2f" % tcmp

        # means: sell,buy,last
        return s,b,tcmp

    def currentStatus(self,quote):
        #
        isin = quote.isin()
        if not self.m_dcmpd.has_key(isin):
            # no data for this quote !
            return "UNKNOWN","::","0.00","0.00","::"
        d = self.m_dcmpd[isin]

        st = d['CSA_IND_ETAT']
        if st==' ' or st=='':
            st = 'OK'
        elif st=='H':
            st = 'SUSPEND+'
        elif st=='B':
            st = 'SUSPEND-'
        elif st=='P':
            st = 'SUSPEND'
        elif st=='S':
            st = 'SUSPEND'

        cl = d['CSA_H_REPRIS_COT']
        if cl=='':
            cl = "::"

        if d.has_key('CSA_RESERV_BAS'):
            bas = d['CSA_RESERV_BAS']
        else:
            bas = "-"

        if d.has_key('CSA_RESERV_HAUT'):
            haut = d['CSA_RESERV_HAUT']
        else:
            haut = "-"

        if self.m_clock.has_key(isin):
            clo = self.m_clock[isin]
        else:
            clo = "::"

        return st,cl,bas,haut,clo

    # ---[ status of quote ] ---

    def hasStatus(self):
        return itrade_config.isConnected()

# ============================================================================
# Export me
# ============================================================================

try:
    ignore(gLiveFortuneo)
except NameError:
    gLiveFortuneo = LiveUpdate_fortuneo()

registerLiveConnector('EURONEXT','PAR',QLIST_ANY,QTAG_LIVE,gLiveFortuneo,bDefault=True)
registerLiveConnector('ALTERNEXT','PAR',QLIST_ANY,QTAG_LIVE,gLiveFortuneo,bDefault=True)
registerLiveConnector('PARIS MARCHE LIBRE','PAR',QLIST_ANY,QTAG_LIVE,gLiveFortuneo,bDefault=True)

# ============================================================================
# Test ME
#
# ============================================================================

def test(ticker):
    if gLiveFortuneo.iscacheddataenoughfreshq():
        data = gLiveFortuneo.getcacheddata(ticker)
        if data:
            debug(data)
        else:
            debug("nodata")

    elif gLiveFortuneo.connect():

        state = gLiveFortuneo.getstate()
        if state:
            debug("state=%s" % (state))

            quote = quotes.lookupTicker(ticker,'EURONEXT')
            data = gLiveFortuneo.getdata(quote)
            if data:
                info(data)
            else:
                print "getdata() failure :-("
                debug("nodata")
            info(gLiveFortuneo.currentClock(quote))
            if data:
                info(gLiveFortuneo.currentNotebook(quote))
                info(gLiveFortuneo.currentTrades(quote))
                info(gLiveFortuneo.currentMeans(quote))
            info(gLiveFortuneo.currentStatus(quote))
        else:
            print "getstate() failure :-("

        gLiveFortuneo.disconnect()
    else:
        print "connect() failure :-("

if __name__=='__main__':
    setLevel(logging.INFO)

    test('SAF')

# ============================================================================
# That's all folks !
# ============================================================================
