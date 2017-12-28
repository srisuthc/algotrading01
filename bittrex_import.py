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
    
    sQuery = """INSERT INTO bittrex.bittrex_historical (primary_currency,secondary_currency"""
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

def import_bittrex(sPrimary_Currency, sSecondary_Currency, sInterval):
    sRequestURL = 'https://bittrex.com/Api/v2.0/pub/market/GetTicks?marketName='
    sRequestURL += sPrimary_Currency+'-'
    sRequestURL += sSecondary_Currency
    sRequestURL += '&tickInterval='+sInterval 
    response = requests.get(sRequestURL)
    data = response.json()
    for i in range (0,len(data['result'])-1,1):
        insert_bittrex(sPrimary_Currency,sSecondary_Currency,sInterval,data['result'][i])    
    time.sleep(1)    
    print sPrimary_Currency+"/"+sSecondary_Currency+" imported"
    

import_bittrex('USDT','BTC','thirtyMin')
import_bittrex('USDT','BCC','thirtyMin')
import_bittrex('USDT','ETH','thirtyMin')
import_bittrex('USDT','LTC','thirtyMin')
import_bittrex('USDT','XRP','thirtyMin')
import_bittrex('USDT','ZEC','thirtyMin')
import_bittrex('BTC','ETH','thirtyMin')
import_bittrex('BTC','BCC','thirtyMin')
import_bittrex('BTC','LTC','thirtyMin')
import_bittrex('BTC','XRP','thirtyMin')
import_bittrex('BTC','ZEC','thirtyMin')
import_bittrex('ETH','BCC','thirtyMin')
import_bittrex('ETH','LTC','thirtyMin')
import_bittrex('ETH','XRP','thirtyMin')
import_bittrex('ETH','ZEC','thirtyMin')
