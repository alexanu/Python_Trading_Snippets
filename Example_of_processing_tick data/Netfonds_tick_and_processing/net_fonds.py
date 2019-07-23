# -*- coding: utf-8 -*-
"""
Created on Tue May 01 15:34:19 2018

@author: jjplombo
"""
import pandas as p

print 'net_fonds: module load....'


def netfonds_p_bid_ask( URL_date_symbol ):
    #url_posdump  = r'http://www.netfonds.no/quotes/posdump.php?date=%s%s%s&paper=%s.%s&csv_format=csv'

    sym_posdump  = p.DataFrame()
    cols_posdump = [ 'bid', 'bid_depth', 'bid_depth_total', 'offer', 'offer_depth', 'offer_depth_total' ]

    try:
        sym_posdump = sym_posdump.append( p.read_csv( URL_date_symbol, index_col=0, header=0, parse_dates=True ) )   
    except Exception as e:
        print( "{} posdump not found".format( URL_date_symbol ) )
        return sym_posdump
    sym_posdump.columns = cols_posdump
    # ~~~~~~~~~~~~~~~~~~
    return sym_posdump
    
    
    
def netfonds_t_trade_dump( URL_date_symbol ):
    #url_tdump = r'http://www.netfonds.no/quotes/tradedump.php?date=%s%s%s&paper=%s.%s&csv_format=csv'

    sym_posdump  = p.DataFrame()
    #  price,quantity,source,buyer,seller,initiator
    cols_posdump = [ 'price', 'quantity', 'source', 'offer', 'buyer', 'initiator' ]

    try:
        sym_posdump = sym_posdump.append( p.read_csv( URL_date_symbol, index_col=0, header=0, parse_dates=True ) )   
    except Exception as e:
        print( "{} tradedump not found".format( URL_date_symbol ) )
        return sym_posdump
    sym_posdump.columns = cols_posdump
    # ~~~~~~~~~~~~~~~~~~
    return sym_posdump