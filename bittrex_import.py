# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import httplib
import urllib2
import hashlib
import hmac
import time
import requests
import ccxt
import json
import MySQLdb

db=MySQLdb.connect(host="127.0.0.1",user="python",
                  passwd="P@ssw0rd",db="bittrex")

c = db.cursor()

def insert_bittrex(Primary_Currency, Secondary_Currency, Interval, data1):
    sClose = data1['C']
    sOpen = data1['O']
    sHigh = data1['H']
    sLow = data1['L']
    sTime = data1['T'].replace('T',' ')
    sBV = data1['BV']
    sV = data1['V']
    
    sQuery = ("INSERT INTO bittrex.bittrex_historical (primary_currency,secondary_currency"+\
              ",close_dt,tz,tickinterval,open_price,high_price,low_price,"+\
              "close_price,volume,base_volume) VALUES ('%s','%s'," % (Primary_Currency, Secondary_Currency)+\
              "'%s','UTC','%s'," % (sTime,Interval)+\
              "%s,%s,%s,%s,%s,%s)" % (sOpen,sHigh,sLow,sClose,sV,sBV)+\
              "ON DUPLICATE KEY UPDATE open_price=%s,high_price=%s,low_price=%s,close_price=%s" % (sOpen,sHigh,sLow,sClose)+\
              ",volume=%s,base_volume=%s;" % (sV,sBV)) 
    #print sQuery
    try:
        c.execute(sQuery)
        db.commit()
    except:     
        db.rollback()

def import_bittrex(sPrimary_Currency, sSecondary_Currency, sInterval):
    sRequestURL = ('https://bittrex.com/Api/v2.0/pub/market/GetTicks?'+\
                   'marketName=%s-%s&tickInterval=%s'   
                  % (sPrimary_Currency, sSecondary_Currency, sInterval))
    response = requests.get(sRequestURL)
    data = response.json()
    for i in range (0,len(data['result'])-1,1):
        insert_bittrex(sPrimary_Currency,sSecondary_Currency,sInterval,data['result'][i])    
    time.sleep(1)    
    print sPrimary_Currency+"/"+sSecondary_Currency+" imported"
    
def import_all(sInterval):
    path = 'C:\\Users\\Charm Srisuthapan\\Workspace\\Python2\\data_collection1\\'
    import_list = open(path+'bittrex.conf','r').readlines()
    for row in import_list:
        tmp1 = row.replace('\n','').split(',')
        import_bittrex(tmp1[0],tmp1[1],sInterval)
#import_bittrex('USDT','BTC','thirtyMin')
