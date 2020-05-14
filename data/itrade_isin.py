#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_isin.py
#
# Description: ISIN - build & Verification (ISO 3166)
#              "modulus 10 double add double"
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
# 2006-06-11    dgil  Wrote it from scratch
# ============================================================================

# ============================================================================
# Imports
# ============================================================================

# python system
import logging
import string

# iTrade system
from itrade_logging import *

# ============================================================================
# isnum
# ============================================================================

def isnum(s):
    if not s:
        return False
    for c in s:
        if not c in string.digits:
            return False
    return True

# ============================================================================
# checkISIN
# ============================================================================

asc = {}
n = 65
for i in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
    asc[i] = n
    n += 1

def _buildVerifCode(code):
    #print '_buildVerifCode(%s)' % code
    myCode = ''
    for myCar in code:
        if isnum(myCar):
            myInt = int(myCar)
        else:
            myInt = asc[myCar] - 55
        myCode = myCode + ("%d" % myInt)
    #print 'code=',code,'len(myCode)=',len(myCode),'myCode=',myCode

    myTotal = 0
    strip = len(myCode) & 1
    for i in range(0,len(myCode)):
        if i%2 == strip:
            myInt = int(myCode[i])
            #print 'myInt=',myInt
        else:
            myInt = int(myCode[i])*2
            #print 'myInt=',myInt
            if myInt>9:
                myInt = (myInt%10) + (myInt//10)
                #print 'myInt-redux=',myInt
        myTotal = myTotal + myInt
        #print 'i=',i,'myTotal=',myTotal
    return (10 - (myTotal%10)) % 10

def checkISIN(isin):
    isin = isin.strip().upper()
    if len(isin)==12:
        tempCK = isin[-1:]
        if isnum(tempCK):
            checkDigit = int(tempCK)
            if checkDigit == _buildVerifCode(isin[:-1]):
                return True

    return False

# ============================================================================
# buildISIN
# ============================================================================

def buildISIN(country,code):
    while (len(country)+len(code))<11:
        code = '0' + code
    code = country+code
    return code + "%d" % _buildVerifCode(code)

# ============================================================================
# filterName
#
# remove any ';' character in the quote name
# ============================================================================

def filterName(name):
    while True:
        pos = name.find(';')
        if pos >-1:
            #print pos,name
            name = name[:pos] + name[pos+1:]
            #print name
        else:
            return name

# ============================================================================
# CINS numbers employ the same Issuer (6 characters)/Issue (2 character &
# check digit) concept espoused by the CUSIP Numbering System, which is
# described in detail on the  following page. It is important to note that
# the first position of a CINS code is always represented by an alpha
# character, signifying the Issuer's country code (domicile) or geographic
# region:
#
#   A = Austria         J = Japan           S = South Africa
#   B = Belgium         K = Denmark         T = Italy
#   C = Canada          L = Luxembourg      U = United States
#   D = Germany         M = Mid-East        V = Africa - Other
#   E = Spain           N = Netherlands     W = Sweden
#   F = France          P = South America   X= Europe-Other
#   G = United Kingdom  Q = Australia       Y = Asia
#   H = Switzerland     R = Norway
#
# The CUSIP number consists of nine characters: a base number of six
# characters known as the issuer number, (the 4th, 5th and/or 6th position may
# be alpha or numeric) and a two character suffix (either numeric or
# alphabetic or both) known as the issue number. The ninth character is a
# check digit
#
# The first issue number for an issuer's equity securities is 10
# ============================================================================

cusip_country = {
    'T' : 'IT',
    'L' : 'LU',
    'G' : 'UK',
    'F' : 'FR',
    'E' : 'ES',
    'D' : 'DE',
    'C' : 'CA',
    'B' : 'BE',
}

def extractCUSIP(ref):
    issuer = ref[:6]
    issue  = ref[6:-1]
    country = issuer[0]
    if cusip_country.has_key(country):
        country = cusip_country[country]
    else:
        country = 'US'
    return country,issuer,issue

# ============================================================================
# Test
# ============================================================================

if __name__=='__main__':
    setLevel(logging.INFO)

    info('test1 FR0000072621 : %s' % checkISIN('FR0000072621'))
    info('test1 FR0000072621 : %s' % buildISIN('FR','07262'))
    info('test2 FR0010209809 : %s' % checkISIN('FR0010209809'))
    info('test2 FR0010209809 : %s' % buildISIN('FR','1020980'))
    info('test3 FR0004154060 : %s' % checkISIN('FR0004154060'))
    info('test3 FR0004154060 : %s' % buildISIN('FR','000415406'))
    info('test4 ES0178430E18 : %s' % checkISIN('ES0178430E18'))
    info('test4 ES0178430E18 : %s' % buildISIN('ES','0178430E1'))
    info('test5 NSCFR0000TF6 : %s' % checkISIN('NSCFR0000TF6'))
    info('test4 NSCFR0000TF6 : %s' % buildISIN('NSCFR','TF'))

# ============================================================================
# That's all folks !
# ============================================================================
