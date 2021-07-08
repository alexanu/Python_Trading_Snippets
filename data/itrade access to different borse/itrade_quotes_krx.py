#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_quotes_krx.py
#
# Description: List of quotes from http://eng.krx.co.kr/: KOREA STOCK EXCHANGE - KOREA KOSDAQ EXCHANGE
#
# Developed for iTrade code (http://itrade.sourceforge.net).
#
# Original template for "plug-in" to iTrade is	from Gilles Dumortier.
# New code for KRX is from Michel Legrand.

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
# 2007-05-15    dgil  Wrote it from scratch
# ============================================================================

# ============================================================================
# Imports
# ============================================================================

# python system
import logging
import re
import thread
import time
import string
import httplib
import urllib2
import cookielib

# iTrade system
import itrade_config
from itrade_logging import *
from itrade_defs import *
from itrade_ext import *
from itrade_connection import ITradeConnection

# ============================================================================
# Import_ListOfQuotes_KRX()
#
# ============================================================================

def Import_ListOfQuotes_KRX(quotes,market='KOREA STOCK EXCHANGE',dlg=None,x=0):
    if itrade_config.verbose:
        print 'Update %s list of symbols' % market
    connection = ITradeConnection(cookies = None,
                               proxy = itrade_config.proxyHostname,
                               proxyAuth = itrade_config.proxyAuthentication,
                               connectionTimeout = itrade_config.connectionTimeout
                               )


    if market=='KOREA STOCK EXCHANGE':
        params = "isu_cd=&gbn=1&market_gubun=1&isu_nm=&sort=&std_ind_cd=&std_ind_cd1=&par_pr=&cpta_scl=&sttl_trm=&lst_stk_vl=1&in_lst_stk_vl=&in_lst_stk_vl2=&cpt=1&in_cpt=&in_cpt2=&nat_tot_amt=1&in_nat_tot_amt=&in_nat_tot_amt2="
        place = 'KRX'
    elif market=='KOREA KOSDAQ EXCHANGE':
        params = "isu_cd=&gbn=2&market_gubun=2&isu_nm=&sort=&std_ind_cd=&std_ind_cd1=&par_pr=&cpta_scl=&sttl_trm=&lst_stk_vl=1&in_lst_stk_vl=&in_lst_stk_vl2=&cpt=1&in_cpt=&in_cpt2=&nat_tot_amt=1&in_nat_tot_amt=&in_nat_tot_amt2="
        place = 'KOS'
    else:
        return False

    def splitLines(buf):
        lines = string.split(buf, '</td></tr>')
        lines = filter(lambda x:x, lines)
        def removeCarriage(s):
            if s[-1]=='\r':
                return s[:-1]
            else:
                return s
        lines = [removeCarriage(l) for l in lines]
        return lines

    url = 'http://eng.krx.co.kr'

    info('Import_ListOfQuotes_KRX_%s:connect to %s' % (market,url))
    
    try:
        data = connection.getDataFromUrl(url)
    except:
        info('Import_ListOfQuotes_KRX_%s:unable to connect :-(' % market)
        return False
    
    cj = None

    urlopen = urllib2.urlopen
    Request = urllib2.Request
    cj = cookielib.LWPCookieJar()
    opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
    urllib2.install_opener(opener)

    req = Request(url)
    handle = urlopen(req)

    cj = str(cj)
    cookie = cj[cj.find('JSESSIONID'):cj.find(' for eng.krx.co.kr/>]>')]
    
    host = 'eng.krx.co.kr'

    url = "/por_eng/corelogic/process/ldr/lst_s_001.xhtml?data-only=true"
    
    headers = { "Host": host
                    , "User-Agent": "Mozilla/5.0 (Windows; U; Windows NT 5.1; fr; rv:1.9.2.3) Gecko/20100401 Firefox/3.6.3 (.NET CLR 3.5.30729)"
                    , "Accept": "text/javascript, text/html, application/xml, text/xml, */*"
                    , "Accept-Language": "fr"
                    , "Accept-Encoding": "gzip,deflate"
                    , "Accept-Charset": "ISO-8859-1,utf-8;q=0.7,*;q=0.7"
                    , "Keep-Alive":115
                    , "Connection": "keep-alive"
                    , "X-Requested-With": "XMLHttpRequest"
                    , "X-Prototype-Version": "1.6.1"
                    , "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
                    , "Referer": "http://eng.krx.co.kr/m6/m6_1/m6_1_1/JHPENG06001_01.jsp"
                    , "Content-Length": len(params)
                    , "Cookie": cookie
                    , "Pragma": "no-cache"
                    , "Cache-Control": "no-cache"

                }

    try:
        conn = httplib.HTTPConnection(host,80)
        conn.request("POST",url,params,headers)
        response = conn.getresponse()
    except:
        debug('Import_ListOfQuotes_KRX unable to connect :-(')
        return False

    debug("status:%s reason:%s" %(response.status, response.reason))
    if response.status != 200:
        debug('Import_ListOfQuotes_KRX:status!=200')
        return False
            
    data = response.read()
    
    # returns the data
   
    lines = splitLines(data)
    n = 0
    isin = ''
    print 'Import_ListOfQuotes_KRX_%s:' % market

    for line in lines:
        ticker = line[8:line.index('</td><td>')]
        name = line[line.find('</td><td>')+9:]
        name = name[:name.find('</td><td>')]
        name = name.replace(',','')
        name = name.replace(';','')
        name = name.replace('&amp',' & ')
        if ticker == '035000':
            name = 'G'+'||'+'R'
        if ticker == '060380':
            name = 'DY S'+'-'+'TEC'
            
        # ok to proceed
        n = n + 1        
        quotes.addQuote(isin=isin, name=name, ticker=ticker,
                        market=market, currency='KRW', place=place, country='KR')
    if itrade_config.verbose:            
        print 'Imported %d lines from %s data.' % (n,market)

    return True

# ============================================================================
# Export me
# ============================================================================

registerListSymbolConnector('KOREA STOCK EXCHANGE','KRX',QLIST_ANY,QTAG_LIST,Import_ListOfQuotes_KRX)
registerListSymbolConnector('KOREA KOSDAQ EXCHANGE','KOS',QLIST_ANY,QTAG_LIST,Import_ListOfQuotes_KRX)

# ============================================================================
# Test ME
# ============================================================================

if __name__=='__main__':

    setLevel(logging.INFO)

    from itrade_quotes import quotes
    
    Import_ListOfQuotes_LSE(quotes,'KOREA STOCK EXCHANGE')
    Import_ListOfQuotes_LSE(quotes,'KOREA KOSDAQ EXCHANGE')
    quotes.saveListOfQuotes()

# ============================================================================
# That's all folks !
# ============================================================================
