# -*- coding: utf-8 -*-


import os


def check_error_file(directory = 'D:\\Financial Data\\Netfonds\\DailyTickDataPull', fl = 'errorlog.txt'):
    
    fullpath = directory+'//'+fl    
    if os.path.isfile(fullpath):
        fileobj = open(fullpath,'r')
        for line in fileobj:
            print 'ERRORLOG:'+line,
        fileobj.close()
    else:
        print "NO ERROR FILE DETECTED"
        print "RUN COMPLETED OK"

    return
    
    
if __name__=='__main__':
    check_error_file()
    print 'test complete'