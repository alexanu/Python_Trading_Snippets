#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
# ============================================================================
# Project Name : iTrade
# Module Name  : itrade_login_boursorama.py
#
# Description: Login to boursorama service
#
# The Original Code is iTrade code (http://itrade.sourceforge.net).
#
# The Initial Developer of the Original Code is	Gilles Dumortier.
#
# Portions created by the Initial Developer are Copyright (C) 2004-2008 the
# Initial Developer. All Rights Reserved.
#
# Contributor(s):
#    Sébastien Renard
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
# 2007-01-05    dgil  Wrote it from scratch
# 2007-05-07    sren  Make it works !
# ============================================================================

# ============================================================================
# Imports
# ============================================================================

# python system
import logging
import re
import os
import mimetypes

# iTrade system
import itrade_config
from itrade_logging import *
from itrade_login import *
from itrade_local import message
from itrade_connection import ITradeConnection, ITradeCookies

# ============================================================================
# Login_boursorama()
#
#   login(user,passwd)
#       store cookie after login
#
#   logout()
#       nop
#
# ============================================================================

class Login_boursorama(object):
    def __init__(self):
        debug('LiveUpdate_boursorama:__init__')
        self.m_default_host = "www.boursorama.fr"
        self.m_login_url   = "https://www.boursorama.fr/logunique.phtml"
        self.m_logged = False
        self.m_cookies=ITradeCookies()
        # Manualy set the cookie that tell boursorama we are a cookie aware browser
        self.m_cookies.set("SUP_COOKIE=OUI")
        self.m_connection = ITradeConnection(cookies = None,
                                           proxy = itrade_config.proxyHostname,
                                           proxyAuth = itrade_config.proxyAuthentication,
                                           connectionTimeout = itrade_config.connectionTimeout
                                           )
        debug('Boursorama login (%s) - ready to run' % self.m_default_host)

    # ---[ properties ] ---

    def name(self):
        return 'boursorama'

    def desc(self):
        return message('login_boursorama_desc')

    # ---[ userinfo ] ---
    def saveUserInfo(self,u,p):
        f = open(os.path.join(itrade_config.dirUserData,'boursorama_userinfo.txt'),'w')
        s = u + ',' + p
        f.write(s)
        f.close()

    def loadUserInfo(self):
        try:
            f = open(os.path.join(itrade_config.dirUserData,'boursorama_userinfo.txt'),'r')
        except IOError:
            return None,None
        s = f.read().strip()
        f.close()
        v = s.split(',')
        if len(v)==2:
            return v[0].strip(),v[1].strip()
        return None,None

    # ---[ login ] ---

    def login(self,u=None,p=None):
        # load username / password (if required)
        if u==None or p==None:
            u,p = self.loadUserInfo()
            if u==None or p==None:
                print 'login: userinfo are invalid - please reenter Access Information'
                return False

        try:
            param={"org" : "/index.phtml?",
                   "redirect" : "",
                   "login" : u,
                   "password" : p,
                   "memo" : "oui",
                   "submit2" : "Valider"}
            buf=self.m_connection.getDataFromUrl(self.m_login_url, data=param)
        except IOError,e:
            print "Exception occured while requesting Boursorama login page : %s" % e
            return False

        print "bourso login response :saved to bourso.html"
        file("bourso.html", "w").write(buf)

        return True

    def logout(self):
        self.m_logged = False
        return True

    def logged(self):
        return self.m_logged

# ============================================================================
# Export me
# ============================================================================

try:
    ignore(gLoginBoursorama)
except NameError:
    gLoginBoursorama = Login_boursorama()

registerLoginConnector(gLoginBoursorama.name(),gLoginBoursorama)

# ============================================================================
# Test me
# ============================================================================

if __name__=='__main__':
    setLevel(logging.INFO)

    print 'use usrdata/boursorama_userinfo.txt to get login/password'
    print ' format is one line with <login>,<password>'
    print
    gLoginBoursorama.login(None,None)
    gLoginBoursorama.logout()

# ============================================================================
# That's all folks !
# ============================================================================
