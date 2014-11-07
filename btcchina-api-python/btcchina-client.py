#!/usr/bin/python
# -*- coding: utf-8 -*-
 
import btcchina
from optparse import OptionParser
import logging 
import os
import datetime

from math import ceil, floor
def float_round(num, places = 0, direction = floor):
  return direction(num * (10**places)) / float(10**places)
 
access_key="d98d0036-04d7-49ea-8065-ce24c4c22e24"
secret_key="e30b5e07-0af6-401f-855b-14179a9632d3"
 
bc = btcchina.BTCChina(access_key,secret_key)

parser = OptionParser()  
parser.add_option("-o", "--operation", dest="operation", help="buy or sell", metavar="OPERATION")
(options, args) = parser.parse_args()

if not options.operation:
  print "OPERATION is required."
  parser.print_help()
  exit(1);
 
''' These methods have no arguments '''
result = bc.get_account_info()
btc = result['balance']['btc']['amount']
#btc = float(btc)
cny = result['balance']['cny']['amount']
#cny = float(cny)

if float(cny) < 0.5 and options.operation == 'buy':
  print "NO MONEY!"
  exit(0);

if float(btc) < 0.0005 and options.operation == 'sell':
  print "NO BTC!"
  exit(0);
 
result = bc.get_market_depth2()
bid = result['market_depth']['bid']
ask = result['market_depth']['ask']
 
# NOTE: for all methods shown here, the transaction ID could be set by doing
#result = bc.get_account_info(post_data={'id':'stuff'})
#print result
 
''' buy and sell require price (CNY, 5 decimals) and amount (LTC/BTC, 8 decimals) '''
#result = bc.buy(500,1)
#print result
if options.operation == 'sell':
  order_id = bc.sell(bid[0]['price'], float_round(float(btc), 4))
  order = bc.get_orders(order_id)
  if order['order']['status'] == 'open':
    result = bc.cancel(order_id)
    if result:
      print "order canceled"
  else:
    print "succeed"
    exit(0)

if options.operation == 'buy':
  order_id = bc.buy(ask[0]['price'], float_round(float(cny)/ask[0]['price'], 4))
  order = bc.get_orders(order_id)
  if order['order']['status'] == 'open':
    result = bc.cancel(order_id)
    if result:
      print "order canceled"
  else:
    print "succeed"
    exit(0)

 
''' cancel requires id number of order '''
#result = bc.cancel(2)
#print result
 
''' request withdrawal requires currency and amount '''
#result = bc.request_withdrawal('BTC',0.1)
#print result
 
''' get deposits requires currency. the optional "pending" defaults to true '''
#result = bc.get_deposits('BTC',pending=False)
#print result
 
''' get orders returns status for one order if ID is specified,
    otherwise returns all orders, the optional "open_only" defaults to true '''
#result = bc.get_orders(2)
#print result
#result = bc.get_orders(open_only=True)
#print result
 
''' get withdrawals returns status for one transaction if ID is specified,
    if currency is specified it returns all transactions,
    the optional "pending" defaults to true '''
#result = bc.get_withdrawals(2)
#print result
#result = bc.get_withdrawals('BTC',pending=True)
#print result
 
''' Fetch transactions by type. Default is 'all'. 
    Available types 'all | fundbtc | withdrawbtc | fundmoney | withdrawmoney | 
    refundmoney | buybtc | sellbtc | tradefee'
    Limit the number of transactions, default value is 10 '''
#result = bc.get_transactions('all',10)
#print result
