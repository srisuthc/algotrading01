# -*- coding: utf-8 -*-
"""
Created on Thu Dec 28 14:54:00 2017

@author: Charm Srisuthapan
"""
import datetime
import httplib
import urllib2
import hashlib
import hmac
import time
import requests
import ccxt
import json
import MySQLdb
import pytz
from dateutil.relativedelta import relativedelta



db=MySQLdb.connect(host="127.0.0.1",user="python",
                  passwd="P@ssw0rd",db="poloniex")

c = db.cursor()

def getInterval(sInterval):
    interval = {
            '300'  : '5 min',
            '900'  : '15 min',
            '1800' : '30 min',
            '7200' : '2 hr',
            '14400': '4 hr',
            '86400': '1 day'}
    try:
        sOut = interval[sInterval]
        return sOut
    except:
        raise ValueError('Incorrect Input')

def insert_poloniex(Primary_Currency, Secondary_Currency, Interval, data1):
    sClose = data1['close']
    sOpen = data1['open']
    sHigh = data1['high']
    sLow = data1['low']
    sTime = datetime.datetime.utcfromtimestamp(data1['date']).strftime('%Y-%m-%d %H:%M:%S')
    sBV = data1['volume']
    sV = data1['quoteVolume']
      
    sQuery = "INSERT INTO poloniex.poloniex_historical (primary_currency,secondary_currency"
    sQuery += ",close_dt,tz,tickinterval,open_price,high_price,low_price,close_price,volume,base_volume) VALUES ("
    sQuery += "'"+Primary_Currency+"','"+Secondary_Currency+"','"
    sQuery += sTime + "','UTC','"+Interval+"',"
    sQuery += str(sOpen) + ","
    sQuery += str(sHigh) + ","
    sQuery += str(sLow) + ","
    sQuery += str(sClose) + ","
    sQuery += str(sV) + ","
    sQuery += str(sBV) + ") "
    sQuery += "ON DUPLICATE KEY UPDATE open_price="+str(sOpen)
    sQuery += ",high_price="+str(sHigh)
    sQuery += ",low_price="+str(sLow)
    sQuery += ",close_price="+str(sClose)
    sQuery += ",volume="+str(sV)
    sQuery += ",base_volume="+str(sBV)+";"
    
    #print sQuery
    try:
        c.execute(sQuery)
        db.commit()
    except:     
        db.rollback()

def import_poloniex(sPrimary_Currency, sSecondary_Currency, sInterval, sBeg, sEnd):

    sRequestURL = 'https://poloniex.com/public?command=returnChartData&currencyPair='
    sRequestURL += sPrimary_Currency+'_'
    sRequestURL += sSecondary_Currency
    sRequestURL += '&start='+sBeg
    sRequestURL += '&end='+sEnd   
    sRequestURL += '&period='+sInterval 
    response = requests.get(sRequestURL)
    data = response.json()
    for i in range (0,len(data)-1,1):
        insert_poloniex(sPrimary_Currency,sSecondary_Currency,getInterval(sInterval),data[i])    
    time.sleep(1)    
    print sPrimary_Currency+"/"+sSecondary_Currency+" imported"

def importAll(sPrimary_Currency, sSecondary_Currency,sInterval):
    dtBeg = getBegDate(sPrimary_Currency, sSecondary_Currency,sInterval)
    dtNow = pytz.utc.localize(datetime.datetime.utcnow())
    while dtBeg < dtNow:
        dtEnd = dtBeg + relativedelta(months=+1)
        print "Importing :",dtBeg, dtEnd
        sBeg = str(int(time.mktime(dtBeg.timetuple())))
        sEnd = str(int(time.mktime(dtEnd.timetuple())))
        import_poloniex(sPrimary_Currency, sSecondary_Currency, sInterval, sBeg, sEnd)
        dtBeg = dtEnd
    
def getBegDate(sPrimary_Currency, sSecondary_Currency,sInterval):
    sQuery = "SELECT max(close_dt) as max_dt FROM poloniex.poloniex_historical WHERE "
    sQuery += "primary_currency = '"+sPrimary_Currency
    sQuery += "' AND secondary_currency ='"+sSecondary_Currency
    sQuery += "' AND tickinterval = '"+getInterval(sInterval)+"';"
    
    dtLastDate = ""

    try:
        c.execute(sQuery)
        db.commit()
        dtLastDate = c.fetchone()[0]
    except:     
        db.rollback()
    
    if dtLastDate == None:
        return pytz.utc.localize(datetime.datetime(2015,1,1))
    else:
        return pytz.utc.localize(dtLastDate)
    
importAll('USDT','BTC','1800')
importAll('USDT','BCH','1800')
importAll('USDT','ETH','1800')
importAll('USDT','LTC','1800')
importAll('USDT','XRP','1800')
importAll('USDT','ZEC','1800')
importAll('BTC','ETH','1800')
importAll('BTC','BCH','1800')
importAll('BTC','LTC','1800')
importAll('BTC','XRP','1800')
importAll('BTC','ZEC','1800')
importAll('ETH','BCH','1800')
importAll('ETH','ZEC','1800')

