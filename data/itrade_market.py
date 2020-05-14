#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_market.py
#
# Description: Market management
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
# 2006-02-2x    dgil  Wrote it from scratch
# ============================================================================

# ============================================================================
# Imports
# ============================================================================

# python system
import logging
import re
import string
from datetime import datetime
import time
import pytz
from pytz import timezone

# iTrade system
from itrade_logging import *
from itrade_local import message
import itrade_csv
from itrade_defs import *
from itrade_connection import ITradeConnection
import itrade_config

# ============================================================================
# ISIN -> MARKET
# ============================================================================

isin_market = {
    'fr': 'EURONEXT',
    'nl': 'EURONEXT',
    'be': 'EURONEXT',
    'pt': 'EURONEXT',
    'qs': 'EURONEXT',
    'it': 'EURONEXT',
    'uk': 'LSE SETSqx',
    'uk': 'LSE SEAQ',
    'us': 'NASDAQ',
    'au': 'ASX',
    'ca': 'TORONTO EXCHANGE',
    'ch': 'SWISS EXCHANGE',
    'it': 'MILAN EXCHANGE',
    'ie': 'IRISH EXCHANGE',
    'es': 'MADRID EXCHANGE',
    'de': 'FRANKFURT EXCHANGE',
    'no': 'OSLO EXCHANGE',
    'se': 'STOCKHOLM EXCHANGE',
    'dk': 'COPENHAGEN EXCHANGE',
    'br': 'SAO PAULO EXCHANGE',
    'hk': 'HONG KONG EXCHANGE',
    'cn': 'SHANGHAI EXCHANGE',
    'cn': 'SHENZHEN EXCHANGE',
    'in': 'NATIONAL EXCHANGE OF INDIA',
    'in': 'BOMBAY EXCHANGE',
    'nz': 'NEW ZEALAND EXCHANGE',
    'ar': 'BUENOS AIRES EXCHANGE',
    'mx': 'MEXICO EXCHANGE',
    'sg': 'SINGAPORE EXCHANGE',
    'kr': 'KOREA STOCK EXCHANGE',
    'kr': 'KOREA KOSDAQ EXCHANGE',
    'at': 'WIENER BORSE',
    'jp': 'TOKYO EXCHANGE',
    'tw': 'TAIWAN STOCK EXCHANGE',
    }

def isin2market(isin):
    if isin:
        cp = isin[0:2].lower()
        if isin_market.has_key(cp):
            return isin_market[cp]
        else:
            # don't know !
            return None
    else:
        # default to EURONEXT !
        return 'EURONEXT'

# ============================================================================
# MARKET -> Default INDICE
# ============================================================================

market_indice = {
    'EURONEXT': 'FR0003500008',#CAC40
    'ALTERNEXT': 'FR0003500008',#CAC40
    'PARIS MARCHE LIBRE': 'FR0003500008',#CAC40
    'BRUXELLES MARCHE LIBRE': 'BE0389555039',#BEL20
    'NASDAQ': 'US6289081050',#NASDAQ 100 INDEX
    'NYSE': 'US0000000001',#NYSE COMPOSITE INDEX
    'AMEX': 'US0000000001',#NYSE COMPOSITE INDEX
    'OTCBB': 'US0000000001',#NYSE COMPOSITE INDEX
    'LSE SETS': 'GB0001383545',#FSTE 100
    'LSE SETSqx': 'GB0001383545',#FSTE 100
    'LSE SEAQ': 'GB0001383545',#FSTE 100
    'ASX': 'AU0000000004',#S&P/ASX 200
    'TORONTO EXCHANGE': 'CA0000017016',#S&P/TSX COMPOSITE INDEX (OFFICIAL)
    'TORONTO VENTURE': 'CA0000017016',#S&P/TSX COMPOSITE INDEX (OFFICIAL)
    'MILAN EXCHANGE': 'IT0004476708',#FTSE ITALIA ALL-SHARE
    'SWISS EXCHANGE': 'CH0009980894',#SWISS MARKET INDEX(PR)
    'IRISH EXCHANGE': 'IE0001477250',#ISEQ-OVERALL PRICE
    'MADRID EXCHANGE': 'ESI500000005',#IBEX 35
    'FRANKFURT EXCHANGE': 'DE0008469008',#DAX
    'OSLO EXCHANGE': 'NO0007035327',#OSLO BØRS BENCHMARK INDEX
    'STOCKHOLM EXCHANGE': 'SE0001809476',#OMX NORDIC 40
    'COPENHAGEN EXCHANGE': 'SE0001809476',#OMX NORDIC 40
    'SAO PAULO EXCHANGE': 'BR0000000001',#BOVESPA INDEX - IBOVESPA
    'HONG KONG EXCHANGE': 'HK0000004322',#HANG SENG INDEX
    'SHANGHAI EXCHANGE': 'CNM000000183',#SSE 180
    'SHENZHEN EXCHANGE': 'CNM0000000C8',#SSE COMPONENT INDEX
    'NATIONAL EXCHANGE OF INDIA': 'IN0000000001',#S&P CNX NIFTY
    'BOMBAY EXCHANGE': 'IN0000000024',#BSE SENSEX 30
    'NEW ZEALAND EXCHANGE': 'NZ0000000002',#NZX 50 INDEX GROSS
    'BUENOS AIRES EXCHANGE': 'ARMERV160025',#MERVAL
    'MEXICO EXCHANGE': 'QS0010040614',#IPC
    'SINGAPORE EXCHANGE': 'XC0009653640',#STRAITS TIMES INDEX
    'KOREA STOCK EXCHANGE' : 'KR0000000001',#KOSPI COMPOSITE
    'KOREA KOSDAQ EXCHANGE' : 'KR0000000002',#KOSDAQ COMPOSITE
    'WIENER BORSE' : 'AT0000999982',#ATX
    'TOKYO EXCHANGE' : 'XC0009692440',#NIKKEI 225
    'TAIWAN STOCK EXCHANGE' : 'TW0000000001',#TAIWAN TAIEX INDEX
    }

def getDefaultIndice(market):
    if market_indice.has_key(market):
        return market_indice[market]
    else:
        # default to CAC
        return 'FR0003500008'

# ============================================================================
# MARKET -> CURRENCY
# ============================================================================

market_currency = {
    'EURONEXT': 'EUR',
    'ALTERNEXT': 'EUR',
    'PARIS MARCHE LIBRE': 'EUR',
    'BRUXELLES MARCHE LIBRE': 'EUR',
    'NASDAQ': 'USD',
    'NYSE': 'USD',
    'AMEX': 'USD',
    'OTCBB': 'USD',
    'LSE SETS': 'GBP',
    'LSE SETSqx': 'GBP',
    'LSE SEAQ': 'GBP',
    'ASX': 'AUD',
    'TORONTO EXCHANGE': 'CAD',
    'TORONTO VENTURE': 'CAD',
    'MILAN EXCHANGE': 'EUR',
    'SWISS EXCHANGE': 'CHF',
    'IRISH EXCHANGE': 'EUR',
    'MADRID EXCHANGE': 'EUR',
    'FRANKFURT EXCHANGE': 'EUR',
    'OSLO EXCHANGE': 'NOK',
    'STOCKHOLM EXCHANGE': 'SEK',
    'COPENHAGEN EXCHANGE': 'DKK',
    'SAO PAULO EXCHANGE': 'BRL',
    'HONG KONG EXCHANGE': 'HKD',
    'SHANGHAI EXCHANGE': 'CNY',
    'SHENZHEN EXCHANGE': 'CNY',
    'NATIONAL EXCHANGE OF INDIA': 'INR',
    'BOMBAY EXCHANGE': 'INR',
    'NEW ZEALAND EXCHANGE': 'NZD',
    'BUENOS AIRES EXCHANGE': 'ARS',
    'MEXICO EXCHANGE': 'MXN',
    'SINGAPORE EXCHANGE': 'SGD',
    'KOREA STOCK EXCHANGE': 'KRW',
    'KOREA KOSDAQ EXCHANGE': 'KRW',
    'WIENER BORSE': 'EUR',
    'TOKYO EXCHANGE': 'JPY',
    'TAIWAN STOCK EXCHANGE': 'TWD',
    }

def market2currency(market):
    if market_currency.has_key(market):
        return market_currency[market]
    else:
        # default to EURO
        return 'EUR'

# ============================================================================
# list_of_markets
# ============================================================================

_lom = {
    'EURONEXT' : False,
    'ALTERNEXT': False,
    'PARIS MARCHE LIBRE': False,
    'BRUXELLES MARCHE LIBRE': False,
    'LSE SETS': False,
    'LSE SETSqx': False,
    'LSE SEAQ': False,
    'NASDAQ': False,
    'NYSE': False,
    'AMEX': False,
    'OTCBB': False,
    'ASX': False,
    'TORONTO EXCHANGE': False,
    'TORONTO VENTURE': False,
    'MILAN EXCHANGE': False,
    'SWISS EXCHANGE': False,
    'IRISH EXCHANGE': False,
    'MADRID EXCHANGE': False,
    'FRANKFURT EXCHANGE': False,
    'OSLO EXCHANGE': False,
    'STOCKHOLM EXCHANGE': False,
    'COPENHAGEN EXCHANGE': False,
    'SAO PAULO EXCHANGE': False,
    'HONG KONG EXCHANGE': False,
    'SHANGHAI EXCHANGE': False,
    'SHENZHEN EXCHANGE': False,
    'NATIONAL EXCHANGE OF INDIA': False,
    'BOMBAY EXCHANGE': False,
    'NEW ZEALAND EXCHANGE': False,
    'BUENOS AIRES EXCHANGE': False,
    'MEXICO EXCHANGE': False,
    'SINGAPORE EXCHANGE': False,
    'KOREA STOCK EXCHANGE': False,
    'KOREA KOSDAQ EXCHANGE': False,
    'WIENER BORSE': False,
    'TOKYO EXCHANGE':False,
    'TAIWAN STOCK EXCHANGE': False,
    }

def set_market_loaded(market, set=True):
    if market in _lom:
        _lom[market] = set
    if itrade_config.verbose:
        print 'Load market %s' % market

def unload_markets():
    for market in _lom:
        _lom[market] = False

def is_market_loaded(market):
    if market in _lom:
        return _lom[market]
    else:
        return False

def list_of_markets(ifLoaded=False,bFilterMode=False):
    lom = []
    if bFilterMode:
        lom.append(message('all_markets'))
    keys = _lom.keys()
    keys.sort()
    for market in keys:
        if not ifLoaded or _lom[market]:
            lom.append(market)
    return lom

# ============================================================================
# use isin / market / place to found the country
# ============================================================================

def compute_country(isin,market,place):
    if isin:
        cp = isin[0:2].upper()
        if cp=='QS':
            return 'FR'
        else:
            return cp
    else:
        if market=='EURONEXT' or market=='ALTERNEXT':
            if place=='PAR': return 'FR'
            if place=='BRU': return 'BE'
            if place=='AMS': return 'NL'
            if place=='LIS': return 'PT'
            return 'FR'
        if market=='PARIS MARCHE LIBRE':
            return 'FR'
        if market=='BRUXELLES MARCHE LIBRE':
            return 'BE'
        if market=='LSE SETS' or market=='LSE SETSqx' or market=='LSE SEAQ':
            return 'GB'
        if market=='NASDAQ':
            return 'US'
        if market=='AMEX':
            return 'US'
        if market=='NYSE':
            return 'US'
        if market=='OTCBB':
            return 'US'
        if market=='ASX':
            return 'AU'
        if market=='TORONTO EXCHANGE' or market=='TORONTO VENTURE':
            return 'CA'
        if market=='SWISS EXCHANGE':
            return 'CH'
        if market=='MILAN EXCHANGE':
            return 'IT'
        if market=='IRISH EXCHANGE':
            return 'IE'
        if market=='MADRID EXCHANGE':
            return 'ES'
        if market=='FRANKFURT EXCHANGE':
            return 'DE'
        if market=='OSLO EXCHANGE':
            return 'NO'
        if market=='STOCKHOLM EXCHANGE':
            return 'SE'
        if market=='COPENHAGEN EXCHANGE':
            return 'DK'
        if market=='SAO PAULO EXCHANGE':
            return 'BR'
        if market=='HONG KONG EXCHANGE':
            return 'HK'
        if market=='SHANGHAI EXCHANGE':
            return 'CN'
        if market=='SHENZHEN EXCHANGE':
            return 'CN'
        if market=='NATIONAL EXCHANGE OF INDIA':
            return 'IN'
        if market=='BOMBAY EXCHANGE':
            return 'IN'
        if market=='NEW ZEALAND EXCHANGE':
            return 'NZ'
        if market=='BUENOS AIRES EXCHANGE':
            return 'AR'
        if market=='MEXICO EXCHANGE':
            return 'MX'
        if market=='SINGAPORE EXCHANGE':
            return 'SG'
        if market=='KOREA STOCK EXCHANGE' or market=='KOREA KOSDAQ EXCHANGE':
            return 'KR'
        if market=='WIENER BORSE':
            return 'AT'
        if market=='TOKYO EXCHANGE':
            return 'JP'
        if market=='TAIWAN STOCK EXCHANGE':
            return 'TW'

    return '??'

# ============================================================================
# list_of_places
# ============================================================================

def list_of_places(market):
    lop = []
    if market=='EURONEXT':
        lop.append('PAR')
        lop.append('BRU')
        lop.append('AMS')
        lop.append('LIS')
    if market=='ALTERNEXT':
        lop.append('PAR')
        lop.append('BRU')
        lop.append('AMS')
        lop.append('LIS')
    if market=='PARIS MARCHE LIBRE':
        lop.append('PAR')
    if market=='BRUXELLES MARCHE LIBRE':
        lop.append('BRU')
    if market=='NASDAQ':
        lop.append('NYC')
    if market=='AMEX':
        lop.append('NYC')
    if market=='NYSE':
        lop.append('NYC')
    if market=='OTCBB':
        lop.append('NYC')
    if market=='LSE SETS':
        lop.append('LON')
    if market=='LSE SETSqx':
        lop.append('LON')
    if market=='LSE SEAQ':
        lop.append('LON')
    if market=='ASX':
        lop.append('SYD')
    if market=='TORONTO EXCHANGE' or market=='TORONTO VENTURE':
        lop.append('TOR')
    if market=='SWISS EXCHANGE':
        lop.append('XSWX')
        lop.append('XVTX')
    if market=='MILAN EXCHANGE':
        lop.append('MIL')
    if market=='IRISH EXCHANGE':
        lop.append('DUB')
    if market=='MADRID EXCHANGE':
        lop.append('MAD')
    if market=='FRANKFURT EXCHANGE':
        lop.append('FRA')
    if market=='OSLO EXCHANGE':
        lop.append('OSL')
    if market=='STOCKHOLM EXCHANGE':
        lop.append('STO')
    if market=='COPENHAGEN EXCHANGE':
        lop.append('CSE')
    if market=='SAO PAULO EXCHANGE':
        lop.append('SAO')
    if market=='HONG KONG EXCHANGE':
        lop.append('HKG')
    if market=='SHANGHAI EXCHANGE':
        lop.append('SHG')
    if market=='SHENZHEN EXCHANGE':
        lop.append('SHE')
    if market=='NATIONAL EXCHANGE OF INDIA':
        lop.append('NSE')
    if market=='BOMBAY EXCHANGE':
        lop.append('BSE')
    if market=='NEW ZEALAND EXCHANGE':
        lop.append('NZE')
    if market=='BUENOS AIRES EXCHANGE':
        lop.append('BUE')
    if market=='MEXICO EXCHANGE':
        lop.append('MEX')
    if market=='SINGAPORE EXCHANGE':
        lop.append('SGX')
    if market=='KOREA STOCK EXCHANGE':
        lop.append('KRX')
    if market=='KOREA KOSDAQ EXCHANGE':
        lop.append('KOS')
    if market=='WIENER BORSE':
        lop.append('WBO')
    if market=='TOKYO EXCHANGE':
        lop.append('TKS')
    if market=='TAIWAN STOCK EXCHANGE':
        lop.append('TAI')

    return lop

# ============================================================================
# market2place
# ============================================================================

market_place = {
    'EURONEXT': 'PAR',
    'ALTERNEXT': 'PAR',
    'PARIS MARCHE LIBRE': 'PAR',
    'BRUXELLES MARCHE LIBRE': 'BRU',
    'NASDAQ': 'NYC',
    'NYSE': 'NYC',
    'AMEX': 'NYC',
    'OTCBB': 'NYC',
    'LSE SETS': 'LON',
    'LSE SETSqx': 'LON',
    'LSE SEAQ': 'LON',
    'ASX': 'SYD',
    'TORONTO EXCHANGE': 'TOR',
    'TORONTO VENTURE': 'TOR',
    'MILAN EXCHANGE': 'MIL',
    'SWISS EXCHANGE': 'XVTX',
    'IRISH EXCHANGE': 'DUB',
    'MADRID EXCHANGE': 'MAD',
    'FRANKFURT EXCHANGE': 'FRA',
    'OSLO EXCHANGE': 'OSL',
    'STOCKHOLM EXCHANGE': 'STO',
    'COPENHAGEN EXCHANGE': 'CSE',
    'SAO PAULO EXCHANGE': 'SAO',
    'HONG KONG EXCHANGE': 'HKG',
    'SHANGHAI EXCHANGE': 'SHG',
    'SHENZHEN EXCHANGE': 'SHE',
    'NATIONAL EXCHANGE OF INDIA': 'NSE',
    'BOMBAY EXCHANGE': 'BSE',
    'NEW ZEALAND EXCHANGE': 'NZE',
    'BUENOS AIRES EXCHANGE': 'BUE',
    'MEXICO EXCHANGE': 'MEX',
    'SINGAPORE EXCHANGE': 'SGX',
    'KOREA STOCK EXCHANGE': 'KRX',
    'KOREA KOSDAQ EXCHANGE': 'KOS',
    'WIENER BORSE': 'WBO',
    'TOKYO EXCHANGE': 'TKS',
    'TAIWAN STOCK EXCHANGE': 'TAI',
    }

def market2place(market):
    if market in market_place:
        return market_place[market]
    else:
        # default to PARIS
        return 'PAR'

# ============================================================================
# convertMarketTimeToPlaceTime
#
#   mtime = HH:MM:ss
#   zone = timezone for the data of the connector (see pytz for all_timezones)
#   place = place of the market
# ============================================================================

place_timezone = {
    "PAR":  "Europe/Paris",
    "BRU":  "Europe/Brussels",
    "MAD":  "Europe/Madrid",
    "AMS":  "Europe/Amsterdam",
    "LON":  "Europe/London",
    "NYC":  "America/New_York",
    "SYD":  "Australia/Sydney",
    "DUB":  "Europe/Dublin",
    "FRA":  "Europe/Berlin",
    "TOR":  "America/Toronto",
    "LIS":  "Europe/Lisbon",
    "MIL":  "Europe/Rome",
    "XSWX": "Europe/Zurich",
    "XVTX": "Europe/Zurich",
    "OSL":  "Europe/Oslo",
    "STO":  "Europe/Stockholm",
    "CSE":  "Europe/Copenhagen",
    "SAO":  "America/Sao_Paulo",
    "HKG":  "Asia/Hong_Kong",
    "SHG":  "Asia/Shanghai",
    "SHE":  "Asia/Hong_Kong",
    "NSE":  "Asia/Colombo",
    "BSE":  "Asia/Colombo",
    "NZE":  "Pacific/Auckland",
    "BUE":  "America/Buenos_Aires",
    "MEX":  "America/Mexico_City",
    "SGX":  "Asia/Singapore",
    "KRX":  "Asia/Seoul",
    "KOS":  "Asia/Seoul",
    "WBO":  "Europe/Vienna",
    "TKS":  "Asia/Tokyo",
    "TAI":  "Asia/Taipei",
    }

def convertConnectorTimeToPlaceTime(mdatetime,zone,place):
    #fmt = '%Y-%m-%d %H:%M:%S %Z%z'

    market_tz = timezone(zone)
    place_tz  = timezone(place_timezone[place])

    market_dt = market_tz.localize(mdatetime)
    #print '*** market time:',market_dt.strftime(fmt)
    place_dt  = place_tz.normalize(market_dt.astimezone(place_tz))
    #print '*** place time:',place_dt.strftime(fmt)
    return place_dt


# ============================================================================
# euronextmic
# ============================================================================

euronext_mic = {
    'EURONEXT.PAR': 'XPAR',
    'EURONEXT.AMS': 'XAMS',
    'EURONEXT.BRU': 'XBRU',
    'EURONEXT.LIS': 'XLIS',
    'ALTERNEXT.PAR': 'ALXP',
    'ALTERNEXT.AMS': 'ALXA',
    'ALTERNEXT.BRU': 'ALXB',
    'ALTERNEXT.LIS': 'ALXL',
    'PARIS MARCHE LIBRE.PAR': 'XMLI',
    'BRUXELLES MARCHE LIBRE.BRU': 'MLXB',
    }

def euronextmic(market,place):
    key = market + '.' + place
    if euronext_mic.has_key(key):
        mic = euronext_mic[key]
        return mic

# ============================================================================
# dateformat
# ============================================================================

date_format = {
    'EURONEXT': ("%d/%m/%Y","%H:%M"),
    'ALTERNEXT': ("%d/%m/%Y","%H:%M"),
    'PARIS MARCHE LIBRE': ("%d/%m/%Y","%H:%M"),
    'BRUXELLES MARCHE LIBRE': ("%d/%m/%Y","%H:%M"),
    'NASDAQ': ("%d %b %Y","%I:%M %p"),
    'NYSE': ("%d %b %Y","%I:%M %p"),
    'AMEX': ("%d %b %Y","%I:%M %p"),
    'OTCBB': ("%d %b %Y","%I:%M %p"),
    'LSE SETS': ("%d %b %Y","%H:%M"),
    'LSE SETSqx': ("%d %b %Y","%H:%M"),
    'LSE SEAQ': ("%d %b %Y","%H:%M"),
    'ASX': ("%d %b %Y","%I:%M %p"),
    'TORONTO EXCHANGE': ("%b %d,%Y","%H:%M"),
    'TORONTO VENTURE': ("%b %d,%Y","%H:%M"),
    'MILAN EXCHANGE': ("%d/%m/%Y","%H:%M"),
    'SWISS EXCHANGE': ("%d/%m/%Y","%H:%M"),
    'IRISH EXCHANGE': ("%d/%m/%Y","%H:%M"),
    'MADRID EXCHANGE': ("%d/%m/%Y","%H:%M"),
    'FRANKFURT EXCHANGE': ("%d/%m/%Y","%H:%M"),
    'OSLO EXCHANGE': ("%d.%m.%Y","%H:%M"),
    'STOCKHOLM EXCHANGE': ("%Y-%m-%d","%H:%M"),
    'COPENHAGEN EXCHANGE': ("%Y-%m-%d","%H:%M"),
    'SAO PAULO EXCHANGE': ("%d/%m/%Y","%H:%M"),
    'HONG KONG EXCHANGE': ("%d/%m/%Y","%H:%M"),
    'SHANGHAI EXCHANGE': ("%Y-%m-%d","%H:%M"),
    'SHENZHEN EXCHANGE': ("%Y-%m-%d","%H:%M"),
    'NATIONAL EXCHANGE OF INDIA': ("%b %d,%Y","%H:%M"),
    'BOMBAY EXCHANGE': ("%d %b %Y","%H:%M"),
    'NEW ZEALAND EXCHANGE': ("%d %b %Y","%I:%M %p"),
    'BUENOS AIRES EXCHANGE': ("%d/%m/%Y","%H:%M"),
    'MEXICO EXCHANGE': ("%d/%m/%Y","%H:%M"),
    'SINGAPORE EXCHANGE': ("%d-%m-%Y","%I:%M %p"),
    'KOREA STOCK EXCHANGE': ("%Y.%m.%d","%H:%M"),
    'KOREA KOSDAQ EXCHANGE': ("%Y.%m.%d","%H:%M"),
    'WIENER BORSE': ("%d/%m/%Y","%H:%M"),
    'TOKYO EXCHANGE': ("%Y/%m/%d","%H:%M"),
    'TAIWAN STOCK EXCHANGE': ("%Y/%m/%d","%H:%M"),
    }


# ============================================================================
# yahooTicker
# ============================================================================

yahoo_suffix = {
    'EURONEXT.PAR': '.PA',
    'EURONEXT.AMS': '.AS',
    'EURONEXT.BRU': '.BR',
    'EURONEXT.LIS': '.LS',
    'ALTERNEXT.PAR': '.PA',
    'ALTERNEXT.AMS': '.AS',
    'ALTERNEXT.BRU': '.BR',
    'ALTERNEXT.LIS': '.LS',
    'PARIS MARCHE LIBRE.PAR': '.PA',
    'BRUXELLES MARCHE LIBRE.BRU': '.BR',
    'LSE SETSqx.LON': '.L',
    'LSE SEAQ.LON': '.L',
    'ASX.SYD': '.AX',
    'TORONTO EXCHANGE.TOR': '.TO',
    'TORONTO VENTURE.TOR': '.V',
    'MILAN EXCHANGE.MIL': '.MI',
    'SWISS EXCHANGE.XSWX': '.SW',
    'SWISS EXCHANGE.XVTX': '.VX',
    'IRISH EXCHANGE.DUB': '.IR',
    'MADRID EXCHANGE.MAD': '.MC',
    'FRANKFURT EXCHANGE.FRA': '.F',
    'OSLO EXCHANGE.OSL': '.OL',
    'STOCKHOLM EXCHANGE.STO': '.ST',
    'COPENHAGEN EXCHANGE.CSE': '.CO',
    'SAO PAULO EXCHANGE.SAO': '.SA',
    'HONG KONG EXCHANGE.HKG': '.HK',
    'SHANGHAI EXCHANGE.SHG': '.SS',
    'SHENZHEN EXCHANGE.SHE': '.SZ',
    'NATIONAL EXCHANGE OF INDIA.NSE': '.NS',
    'BOMBAY EXCHANGE.BSE': '.BO',
    'NEW ZEALAND EXCHANGE.NZE': '.NZ',
    'BUENOS AIRES EXCHANGE.BUE': '.BA',
    'MEXICO EXCHANGE.MEX': '.MX',
    'SINGAPORE EXCHANGE.SGX': '.SI',
    'KOREA STOCK EXCHANGE.KRX': '.KS',
    'KOREA KOSDAQ EXCHANGE.KOS': '.KQ',
    'WIENER BORSE.WBO': '.VI',
    'TOKYO EXCHANGE.TKS': '.T',
    'TAIWAN STOCK EXCHANGE.TAI': '.TW',
    }

def yahoosuffix(market,place):
    key = market + '.' + place
    if yahoo_suffix.has_key(key):
        return yahoo_suffix[key]
    else:
        return None

yahoo_map_tickers = {}

def yahooTicker(ticker,market,place):
    # special case for US markets
    if ticker[0:1] == '^':
        return ticker

    pticker = ticker

    # special case for TORONTO
    if market=='TORONTO EXCHANGE' or market=='TORONTO VENTURE':
        s = ticker.split('-')
        if len(s)==3:
            ticker = s[0]+'-'+s[1]+s[2]

    # special case for AMEX
    if market=="AMEX":
        s = ticker.split('.')
        if len(s)==2:
            ticker = s[0]+'-'+s[1]
            if s[1]=="W":
                ticker = ticker + "T"
        else:
            s = ticker.split('-')
            if len(s)==2:
                ticker= s[0]+'-P'+s[1]

    #if pticker!=ticker and itrade_config.verbose:
    #    print 'convert to Yahoo ticker %s -> %s' % (pticker,ticker)

    # build the ticker using the suffix
    key = market + '.' + place
    if yahoo_suffix.has_key(key):
        sticker = ticker + yahoo_suffix[key]
    else:
        sticker = ticker

    # check if we need to translate to something different !
    if yahoo_map_tickers.has_key(sticker):
        return yahoo_map_tickers[sticker]

    return sticker

infile = itrade_csv.read(None,os.path.join(itrade_config.dirSysData,'yahoo_tickers.txt'))
if infile:
    # scan each line to read each quote
    for eachLine in infile:
        item = itrade_csv.parse(eachLine,2)
        if item:
            yahoo_map_tickers[item[0]] = item[1].strip().upper()


# ============================================================================
# yahooUrl
#
# some marketplaces seems to use various URL from Yahoo website :-(
# ============================================================================

def yahooUrl(market,live):
    if live:
        if market in ['TORONTO VENTURE','TORONTO EXCHANGE']:
            url = "http://download.finance.yahoo.com/d/quotes.csv"
        else:
            #url = "http://quote.yahoo.com/download/quotes.csv"
            url = "http://download.finance.yahoo.com/d/quotes.csv"
    else:
        if market in ['TORONTO VENTURE','TORONTO EXCHANGE']:
            #url = 'http://download.finance.yahoo.com/d/quotes.csv'
            url = 'http://ichart.finance.yahoo.com/table.csv'
            #url = 'http://download.finance.yahoo.com/d/quotes.csv'
        else:
            url = 'http://ichart.finance.yahoo.com/table.csv'

    return url

def yahooUrlJapan(market,live):
    if live:
        url = "http://quote.yahoo.co.jp/q"
        #url = 'http://quote.yahoo.co.jp/q?s=%s&d=v2' % (ss)
    else:
        url = "http://table.yahoo.co.jp/t"
        #url = 'http://table.yahoo.co.jp/t?c=%s&a=%s&b=%s&f=%s&d=%s&e=%s&g=d&s=%s.t&y=%s&z=%s.t' % (d1[0],d1[1],d1[2],d2[0],d2[1],d2[2],ss,str(cursor),ss)
    return url


# ============================================================================
# euronext_IntrusmentId()
# ============================================================================

def euronext_InstrumentId(quote):

    deprecated

    #
    if quote.list()==QLIST_INDICES:
        urlid = 'http://www.euronext.com/quicksearch/resultquicksearchindices-7000-EN.html?matchpattern=%s&fromsearchbox=true&path=/quicksearch&searchTarget=quote'
    else:
        urlid = 'http://www.euronext.com/quicksearch/resultquicksearch-2986-EN.html?matchpattern=%s&fromsearchbox=true&path=/quicksearch&searchTarget=quote'

    connection = ITradeConnection(cookies = None,
                               proxy = itrade_config.proxyHostname,
                               proxyAuth = itrade_config.proxyAuthentication,
                               connectionTimeout = itrade_config.connectionTimeout
                               )

    # get instrument ID
    IdInstrument = quote.get_pluginID()
    if IdInstrument == None:

        try:
            f = open(os.path.join(itrade_config.dirCacheData,'%s.id' % quote.key()),'r')
            IdInstrument = f.read().strip()
            f.close()
            #print "euronext_InstrumentId: get id from file for %s " % quote.isin()
        except IOError:
            #print "euronext_InstrumentId: can't get id file for %s " % quote.isin()
            pass

        if IdInstrument == None:
            url = urlid % quote.isin()

            if itrade_config.verbose:
                print "euronext_InstrumentId: urlID=%s " % url

            try:
                buf=connection.getDataFromUrl(url)
            except:
                print 'euronext_InstrumentId: %s exception error' % url
                return None
            sid = re.search(r"selectedMep=%d&amp;idInstrument=\d*&amp;isinCode=%s" % (euronext_place2mep(quote.place()),quote.isin()), buf, re.IGNORECASE|re.MULTILINE)
            if sid:
                sid = buf[sid.start():sid.end()]
                #print'seq-1 found:',sid
                sexch = re.search(r"&amp;isinCode", sid, re.IGNORECASE|re.MULTILINE)
                if sexch:
                    IdInstrument = sid[31:sexch.start()]
                    #print 'seq-2 found:',IdInstrument
                else:
                    print 'euronext_InstrumentId: seq-2 not found : &amp;isinCode'
            else:
                print 'euronext_InstrumentId: seq-1 not found : selectedMep=%d&amp;idInstrument=\d*&amp;isinCode=%s' % (euronext_place2mep(quote.place()),quote.isin())
                #print buf
                #exit(0)

        if IdInstrument == None:
            print "euronext_InstrumentId:can't get IdInstrument for %s " % quote.isin()
            return None
        else:
            if itrade_config.verbose:
                print "euronext_InstrumentId: IdInstrument for %s is %s" % (quote.isin(),IdInstrument)
            quote.set_pluginID(IdInstrument)
            try:
                f = open(os.path.join(itrade_config.dirCacheData,'%s.id' % quote.key()),'w')
                f.write('%s' % IdInstrument)
                f.close()
            except IOError:
                #print "euronext_InstrumentId: can't write id file for %s " % quote.isin()
                pass

    return IdInstrument

# ============================================================================
# That's all folks !
# ============================================================================
