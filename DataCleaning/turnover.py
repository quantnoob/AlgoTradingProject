#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 28 21:22:56 2022

@author: Haoyang Li
"""

import pandas as pd
import numpy as np

def weight_calc(price,shares):
    '''
    Calculate the weights of stocks for a specific date
    price - data series of each stock's price
    shares - data series of the numbers of shares each stock
    return: a pandas series of weight
    '''
    caps = price*shares
    total_cap = caps.sum()
    return price*shares / total_cap

def create_date_table(df):
    '''
    splite the sp500 table by dates into a list of pandas dataframes
    need type(df['Date']) = int
    return: a list of data frames, each for a specific date.
    '''
    # create a list of sorted dates
    dates = np.sort(pd.unique(df['Date']))
    tables = []
    for date in dates:
        temp = df[(df['Date'] == date)]
        temp['Weight'] = weight_calc(temp['Price'],temp['Shares'])
        tables.append(temp)
    return tables

value = 10**8

def turnover_calc(df1,df2):
    '''
    calculate turnover rate for buy/sold between dates for df1 and df2
    df1, df2 - the input data tables
    return: the turnover rate for buying and the turnover rate for selling
    '''
    #calculate the shares of each stock in the portfolio for a specific date
    df1['Share_port'] = df1['Weight']*value/df1['Price']
    df2['Share_port'] = df2['Weight']*value/df2['Price']
    #outer join the two tables to compare the shares of each stock and reindex the joined table
    merged = df1.merge(df2, how='outer', left_on='Ticker', right_on='Ticker')
    merged['index'] = range(merged.shape[0])
    merged.set_index('index')
    #for a stock existing in only one of the table, we put 0 for the number of shares for another table
    merged['Share_port_x'].fillna(0,inplace=True)
    merged['Share_port_y'].fillna(0,inplace=True)
    #we use price from the later date to calculate the transaction price
    #if that is empty, use the one from the earlier date
    for i in range(merged['Price_y'].shape[0]):
        if (pd.isnull(merged['Price_y'][i])):
            merged['Price_y'][i] = merged['Price_x'][i]
    #calculate the difference of holdings and sum the buy and sell
    diff = (merged['Share_port_y']-merged['Share_port_x'])*merged['Price_y']
    merged['diff'] = diff
    #print(merged)
    #calculate turnover rate
    buy = diff[diff>0].sum()
    sell = diff[diff<0].sum()
    return buy/value,sell/value

if __name__ == '__main__':
    #read the sp500 data
    sp500_d = pd.read_excel("s&p500.xlsx", usecols='B,H,I,AU,AZ', names=['Date', 'Ticker','Name','Price','Shares'])
    
    #change the float to int in Date column and forward fill the Price and Shares
    sp500 = sp500_d[sp500_d.Date.notnull()]
    sp500['Date'] = sp500['Date'].astype(int)
    sp500['Price'].fillna(method='ffill', inplace=True)
    sp500['Shares'].fillna(method='ffill', inplace=True)
    
    #select June 20th and September 20th.
    jun_date = 20070620
    sep_date = 20070920
    jun = sp500[(sp500['Date'] == jun_date)]
    sep = sp500[(sp500['Date'] == sep_date)]
    
    #calculate the holdings at 2007/06/20 and 2007/09/20
    jun['Weight'] = weight_calc(jun['Price'],jun['Shares'])
    sep['Weight'] = weight_calc(sep['Price'],sep['Shares'])
    
    #write the result to csv file
    f1 = 'jun_20_holdings.csv'
    jun.to_csv(f1)
    f2 = 'sep_20_holdings.csv'
    sep.to_csv(f2)
    
    #calculate the turnover rate if change portfolio every day
    tbs = create_date_table(sp500)
    bs = 0
    ss = 0
    for i in range(len(tbs)-1):
        buy,sell = turnover_calc(tbs[i],tbs[i+1])
        bs += buy
        ss += sell
    to_rate = min(bs, abs(ss))*4   #converting to annual rate by multiplying by 4
    print('If we change the portfolio according to sp500 index every day, turnover rate is ',
          to_rate)
    
    #calculate the turnover rate if only consider 2007/06/20 and 2007/09/20
    buy, sell = turnover_calc(jun,sep)
    to_rate_2 = min(buy, abs(sell))*4
    print('If we build the portfolio at 2007/06/20 and only change it at 2007/09/20, turnover rate is ',
          to_rate_2)

