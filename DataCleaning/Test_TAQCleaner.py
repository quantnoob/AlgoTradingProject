import unittest
import pandas as pd
import numpy as np
import datetime
from TAQCleaner import TAQCleaner
import gzip
import struct
import os

def generate_fake_data(workingDir,type = None):
    if not os.path.exists(workingDir):
        os.makedirs(workingDir)
    if type == 'trade':
        ## prices
        prices = [1]*20 + [999] + [1] * 20
        ## seconds to epoch, number of data samples
        header = [1182312000, len(prices)]
        ## sized
        sizes = [i*10 for i in range(len(prices))]
        ##ts
        ts = [3420000 + i*1000 for i in range(len(prices))]
        tradeFilePath = os.path.join(workingDir,'trade_SP_Adj','20070620')
        tradeDataPath = os.path.join(tradeFilePath,'FAKE_trades.binRT')
        if not os.path.exists(tradeFilePath):
            os.makedirs(tradeFilePath)
        out = gzip.open(tradeDataPath,"wb")
        out.write(struct.pack(">2i",header[0], header[1]))
        out.write(struct.pack(">%di" % len(ts), *ts))
        out.write(struct.pack(">%df" % len(sizes), *sizes))
        out.write(struct.pack(">%df" % len(prices), *prices))
        out.close()
        return
    if type == 'quote':
        ## prices
        bid_prices = [1-0.1]*20 + [999-0.1] + [1-0.1] * 20
        ask_prices = [1+0.1]*20 + [999+0.1] + [1+0.1] * 20
        ## seconds to epoch, number of data samples
        header = [1182312000, len(bid_prices)]
        ## sized
        bid_sizes = [i*10 for i in range(len(bid_prices))]
        ask_sizes = bid_sizes
        ##ts
        ts = [3420000 + i*1000 for i in range(len(bid_prices))]
        quoteFilePath = os.path.join(workingDir,'quote_SP_Adj','20070620')
        quoteDataPath = os.path.join(quoteFilePath,'FAKE_quotes.binRQ')
        if not os.path.exists(quoteFilePath):
            os.makedirs(quoteFilePath)
        out = gzip.open(quoteDataPath,"wb")
        out.write(struct.pack(">2i",header[0], header[1]))
        out.write(struct.pack(">%di" % len(ts), *ts))
        out.write(struct.pack(">%df" % len(bid_sizes), *bid_sizes))
        out.write(struct.pack(">%df" % len(bid_prices), *bid_prices))
        out.write(struct.pack(">%df" % len(ask_prices), *ask_sizes))
        out.write(struct.pack(">%df" % len(ask_prices), *ask_prices))
        out.close()
        return
    raise ValueError('Wrong type. trade or quote.')
    

    
class Test_TAQCleaner(unittest.TestCase):
    def fake_data_generate(self):
        workingDir = '/Users/barry/Desktop/NYU Courses/courseSpring2022/algo trading/hw1/FakeData'
        dirSuffix = '/DataSet'
        workingDir = workingDir + dirSuffix
        try:
            generate_fake_data(workingDir,type='trade')
        except ValueError:
            print('Wrong with fake trade data generate')
        try:
            generate_fake_data(workingDir,type='quote')
        except ValueError:
            print('Wrong with fake quote data generate')

    def testCleaner(self):
        self.fake_data_generate()
        workingDir = '/Users/barry/Desktop/NYU Courses/courseSpring2022/algo trading/hw1/FakeData'
        tradeDir = os.path.join(workingDir,'DataSet/trade_SP_Adj/20070620/FAKE_trades.binRT')
        quoteDir = os.path.join(workingDir, 'DataSet/quote_SP_Adj/20070620/FAKE_quotes.binRQ')
    
        trade_cleaner = TAQCleaner(tradeDir, type = 'trade')
        quote_cleaner = TAQCleaner(quoteDir, type= 'quote')

        trade_cleaner.processPrices()
        trade_cleaner.processTimestamps()
        trade_cleaner.cleaningData(K=21, gamma_multiplier=0.00005)

        quote_cleaner.processPrices()
        quote_cleaner.processTimestamps()
        quote_cleaner.cleaningData(K=21, gamma_multiplier=0.00005)

        prices = [1]*20 + [999] + [1] * 20

        bid_prices = [1-0.1]*20 + [999-0.1] + [1-0.1] * 20
        ask_prices = [1+0.1]*20 + [999+0.1] + [1+0.1] * 20

        self.assertEquals(trade_cleaner.getPrices()[-1], 1.0)
        self.assertEquals(trade_cleaner.dataReader._ts[0], 3420000)
        self.assertEquals(trade_cleaner.outlierIdx[0], prices.index(999))

        ## the quote cleaner return mid price
        self.assertEquals(quote_cleaner.getPrices()[-1], (bid_prices[-1] + ask_prices[-1])/2)
        self.assertAlmostEquals(quote_cleaner.getAskPrices()[-1], 1.1)
        self.assertAlmostEquals(quote_cleaner.getBidPrices()[-1], 0.9)
        self.assertEquals(quote_cleaner.dataReader._ts[0], 3420000)
        self.assertEquals(quote_cleaner.outlierIdx[0], prices.index(999))

        trade_cleaner.plotCleaningTradesResultGraph()
        quote_cleaner.plotCleaningQuotesResultGraph()

if __name__ == '__main__':
    workingDir = '/Users/barry/Desktop/NYU Courses/courseSpring2022/algo trading/hw1/FakeData'
    dirSuffix = '/DataSet'
    workingDir = workingDir + dirSuffix
    try:
        generate_fake_data(workingDir,type='trade')
    except ValueError:
        print('Wrong with fake trade data generate')
    try:
        generate_fake_data(workingDir,type='quote')
    except ValueError:
        print('Wrong with fake quote data generate')