#install mysqlclient
import MySQLdb as mdb
import pandas as pd
import Quandl as q
import time
import datetime

scriptStart = time.time()

token = "Xxxxx"

#connect to DB
def connect_to_DB():
    #Connect to the <a href="http://.../mysql/">MySQL</a> instance
    db_host = '127.0.0.1'
    db_user = 'root'
    db_pass = 'xxx'
    db_name = 'lean'
    
    con = mdb.connect(host = db_host, user = db_user, passwd = db_pass, db = db_name)
    
    return con

#create requiredtable
def create_table(tableName, con):
    str_ = """CREATE TABLE IF NOT EXISTS """+ tableName + """ (DATE_TIME timestamp NOT NULL, CLOSE double, \
        HIGH double, LOW double, \
        OPEN double, VOLUME double, ADJCLOSE double, \
        PRIMARY KEY  (DATE_TIME)) ENGINE=InnoDB AUTO_INCREMENT=0;"""
    
    with con: 
        cur = con.cursor()
        cur.execute(str_)
        
#check for not available data from Quandl
def empty(df):
    if df.empty:
        result=0
    else:
        result=1
    return result

def insert_data(quandl_data, cols, digits, con):
    
    for b in range(0, len(quandl_data)):
        print "-----------------------------------------------"
        print "Row: %s" %b

        date1 = str(quandl_data.index[b].year)+"-"+str(quandl_data.index[b].month)+"-"+str(quandl_data.index[b].day)+" 00:00:00"
            
        #make list of values
        var_list = []
        var_string = ""
        col_count = 0
        not_nuls = 0
        nuls = ""
        cnt = 0
        i = 0
        
        for x in cols:
            col_count += 1
            if x != 0:
                not_nuls += 1
                var_string += "%s,"
                var_list.append(round(quandl_data.ix[b].loc[x], digits))
                data_tupple = tuple(var_list)
        
        cnt = col_count - not_nuls
        
        #check if there 6 columns or less
        if not_nuls < col_count:
            for i in range(0, cnt):
                i += 1
                nuls += ",NULL"
            data_str = var_string[:-1] % data_tupple + nuls + ");"
        else:
            data_str = var_string[:-1] % data_tupple + ");"
        
        str_ = """INSERT INTO `""" +tableName+ """` VALUES (\'"""+  date1 + """\',"""+data_str
        print "Executing: "+str_
            
        #execute query with conenction
        try:
            with con:
                cur = con.cursor()
                cur.execute(str_)
        except:
            print "Skipped, database up to date or some problem."

#Date Open High Low Close
def request_Quandl(symbol, tableName, cols, digits):        
    
    con = connect_to_DB()
    
    create_table(tableName, con)
    
    str_ = """SELECT * FROM `""" +tableName+ """` ORDER BY DATE_TIME DESC LIMIT 1;"""
    print str_
    
    data = pd.read_sql_query(str_, con=con, index_col='DATE_TIME')
    
    if empty(data) == 0:
        #execute query with conenction
        quandl_data = q.get(symbol, returns='pandas', authtoken=token, sort_order='asc')
        if empty(quandl_data) == 1:
            print quandl_data.tail()
            print "Data length: %s" % len(quandl_data)
            insert_data(quandl_data, cols, digits, con)
    else:
        last_date = str(data.index[0].year)+"-"+str(data.index[0].month)+"-"+str(data.index[0].day+1)
        print "Last date "+last_date
        quandl_data = q.get(symbol, returns='pandas', authtoken=token, trim_start=last_date, sort_order='asc')
        if empty(quandl_data) == 1:
            print quandl_data.tail()
            print "Data length: %s" % len(quandl_data)
            insert_data(quandl_data, cols, digits, con)

#1st version
yahoosyms = ["YAHOO/INDEX_GSPC"]
for s in yahoosyms:
    print "Symbol: "+s
    tableName = s[:5] + '_' + s[6:]
    print "Table name: "+tableName
    request_Quandl(s, tableName, cols=["Open", "High", "Low", "Close", "Volume", "Adj Close"], digits=2)
    print "Done for "+s
    
#2nd version
syms = ["YAHOO/INDEX_VIX",
        "YAHOO/INDEX_VDAX",
        "YAHOO/INDEX_GVZ"]
for s in syms:
    print "Symbol: "+s
    tableName = s[:5] + '_' + s[6:]
    print "Table name: "+tableName
    request_Quandl(s, tableName, cols=["Open", "High", "Low", "Close", "Volume", "Adjusted Close"], digits=2)
    print "Done for "+s

#3d version
cboesyms = ["CBOE/VXN",
            "CBOE/VXGOG",
            "CBOE/VXAPL",
            "CBOE/VXAZN",
            "CBOE/VXGS",
            "CBOE/VXSLV",
            "CBOE/VXD",
            "CBOE/RVX",
            "CBOE/VXGS",
            "CBOE/VXFXI"]
for s in cboesyms:
    print "Symbol: "+s
    tableName = s[:4] + '_' + s[5:]
    print "Table name: "+tableName
    request_Quandl(s, tableName, cols=["Open", "High", "Low", "Close", 0, 0], digits=2)
    print "Done for "+s

# 4th version
bigsyms = ["CBOE/VXV"]
for s in bigsyms:
    print "Symbol: "+s
    tableName = s[:4] + '_' + s[5:]
    print "Table name: "+tableName
    request_Quandl(s, tableName, cols=["OPEN", "HIGH", "LOW", "CLOSE", 0, 0], digits=2)
    print "Done for "+s
    
#symbols with one value
syms2 = ["CBOE/VVIX"]
for s in syms2:
    print "Symbol: "+s
    tableName = s[:4] + '_' + s[5:]
    print "Table name: "+tableName
    request_Quandl(s, tableName, cols=["VVIX", 0, 0, 0, 0, 0], digits=2)
    print "Done for "+s

syms3 = ["CBOE/EVZ"]
for s in syms3:
    print "Symbol: "+s
    tableName = s[:4] + '_' + s[5:]
    print "Table name: "+tableName
    request_Quandl(s, tableName, cols=["EVZ", 0, 0, 0, 0, 0], digits=2)
    print "Done for "+s

syms4 = ["CBOE/OVX"]
for s in syms4:
    print "Symbol: "+s
    tableName = s[:4] + '_' + s[5:]
    print "Table name: "+tableName
    request_Quandl(s, tableName, cols=["USO VIX (OVX)", 0, 0, 0, 0, 0], digits=2)
    print "Done for "+s

timeused = (time.time()-scriptStart)/60

print("Done in ",timeused, " minutes")
