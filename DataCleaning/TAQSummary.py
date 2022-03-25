import pandas as pd
import numpy as np
from TAQTradesReader import TAQTradesReader as tr
from TAQQuotesReader import TAQQuotesReader as qr
import os
import sys
import platform
import datetime

from tqdm import tqdm
import multiprocessing as mp


class TAQSummary(object):
    def __init__(self,Ticker,workdingDir, freq = 10):
        self.X = freq
        self.Ticker = Ticker
        self.workingDir = workdingDir

    ## load trades data file paths of the given tickers
    def loadTradeData(self, ifCleaned = False, K=None, gamma_multiplier =None):
        if K is None and gamma_multiplier is None:
            if ifCleaned:
                dir_suffix = 'trade_SP_Adj_cleaned'
            else:
                dir_suffix = 'trade_SP_Adj'
        else:
            if ifCleaned:
                dir_suffix = 'trade_SP_Adj_cleaned_' +str(K)+'_'+str(gamma_multiplier).replace('.','_') 
            else:
                dir_suffix = 'trade_SP_Adj'
        
        self.tradeDir = self.workingDir+dir_suffix
     
        dateList = [i for i in os.listdir(self.tradeDir) if os.path.isdir(os.path.join(self.tradeDir,i))]
        dateList.sort()

        self.tradeFileList = []
        for _date in dateList:
            _tempDir = os.path.join(self.tradeDir, _date, self.Ticker + '_trades.binRT')
            if os.path.exists(_tempDir):
                self.tradeFileList.append(_tempDir)

    ## load quotes data file paths of the given tickers
    def loadQuoteData(self, ifCleaned = False,K=None, gamma_multiplier = None):
        if K is None and gamma_multiplier is None:
            if ifCleaned:
                dir_suffix = 'quote_SP_Adj_cleaned'
            else:
                dir_suffix = 'quote_SP_Adj'
        else:
            if ifCleaned:
                dir_suffix = 'quote_SP_Adj_cleaned_' +str(K)+'_'+str(gamma_multiplier).replace('.','_') 
            else:
                dir_suffix = 'quote_SP_Adj'
        self.quoteDir = self.workingDir+dir_suffix
        dateList = [i for i in os.listdir(self.quoteDir) if os.path.isdir(os.path.join(self.quoteDir,i))]
        dateList.sort()

        self.quoteFileList = []
        for _date in dateList:
            _tempDir = os.path.join(self.quoteDir, _date, self.Ticker + '_quotes.binRQ')
            if os.path.exists(_tempDir):
                self.quoteFileList.append(_tempDir)
    
    ## compute the returns of given ticker for given file path
    def computeStatWithFreq(self, filePathName, type='trade'):

        X = self.X
        ## given a filePathName read the data
        ## then calcualte return
        if type == 'trade':
            dataReader = tr(filePathName)
        else:
            dataReader = qr(filePathName)
        ## Time
        _tsYearMonthDate = datetime.datetime.fromtimestamp(
                dataReader.getSecsFromEpocToMidn()
        )
        _tsList = []
        for i in range(dataReader.getN()):
            _tsList.append(
                ## Y-M-D + m-s
                pd.Timestamp(_tsYearMonthDate+\
                    datetime.timedelta(milliseconds = 
                        dataReader.getMillisFromMidn(i)))
            )
        ## Prices
        _prices = []
        if type == 'trade':
            for i in range(dataReader.getN()):
                ## add trade price
                _prices.append(dataReader.getPrice(i)) 
        else:
            for i in range(dataReader.getN()):
                ## add mid price
                _midPrice = 1/2 * (dataReader.getAskPrice(i) +\
                            dataReader.getBidPrice(i))
                _prices.append(_midPrice) 
        ## Time-Price DataFrame
        _df = pd.DataFrame({'Prices':_prices,'Time':_tsList}).set_index('Time')
   
        ## resample data using the given frequency
        _dfResampled = _df.resample(str(X)+'s').first().pct_change()
        #_returns = _dfResampled['Prices'].dropna().to_list()

        ## return the resampled returns in a list, also the number of trades/quotes with date
        return _dfResampled, dataReader.getN()

    ## compute summary statistics based on returns calcualted from each individual Ticker,
    ## on individual dates
    def computeStatForAllDatesWithFreq(self, ifCleaned = False, K=None, gamma_multiplier = None):
        X = self.X
        ## compute returns of given ticker with all other statistics
        ## calculate the how period, ignoring the timestamp
        self.loadQuoteData(ifCleaned=ifCleaned, K=K, gamma_multiplier = gamma_multiplier)
        self.loadTradeData(ifCleaned=ifCleaned, K=K, gamma_multiplier = gamma_multiplier)
        self.trade_returns = []
        self.trade_nums = 0 
        self.quote_returns = []
        self.quote_nums = 0

        pbarTrade = tqdm(total=len(self.tradeFileList))
        pbarTrade.set_description('processing trade stat')
        pbarQuote = tqdm(total=len(self.quoteFileList))
        pbarQuote.set_description('processing quote stat')

        def updateTrade(x):
            self.trade_returns.append(x[0])
            self.trade_nums += x[1]
            pbarTrade.update()

        def updateQuote(x):
            self.quote_returns.append(x[0])
            self.quote_nums += x[1]
            pbarQuote.update()

        n_core = 10
        pool1 = mp.Pool(n_core)
        pool2 = mp.Pool(n_core)
        
        for _param1, _param2 in zip(self.tradeFileList, self.quoteFileList):
            pool1.apply_async(self.computeStatWithFreq,
                             args=(_param1,'trade'),callback=updateTrade)
            pool2.apply_async(self.computeStatWithFreq,
                             args=(_param2,'quote'),callback=updateQuote)
        pool1.close()
        pool1.join()
        pool2.close()
        pool2.join()

    ## summary all the data computed so far
    def computeSummary(self):
        def calculateMaximumDrawDown(df):
            arr_list =(df['Prices']+1).tolist()
            arr_list.insert(0, 1)
            _max = arr_list[0]
            _dd = []
            for i in range(len(arr_list)):
                if arr_list[i]>=_max:
                    _dd.append(0)
                    _max = arr_list[i]
                else:
                    _dd.append((_max - arr_list[i])/_max)
            return max(_dd)

        def calculateStats(df):
            trading_seconds = (16-9.5)*3600
            _mean = df['Prices'].mean() / self.X * trading_seconds * 252
            _median = df['Prices'].median()/ self.X * trading_seconds * 252
            _std = df['Prices'].std() * np.sqrt(trading_seconds * 252 / self.X)
            _mad = df['Prices'].mad() / self.X * trading_seconds * 252
            _skew = df['Prices'].skew()
            _kurt = df['Prices'].kurt()
            tenLarge = df['Prices'].nlargest(10)
            tenSmall = df['Prices'].nsmallest(10)
            _mdd = calculateMaximumDrawDown(df)
            return [_mean, _median,_std, _mad, _skew,_kurt, _mdd, tenLarge, tenSmall]
            
        ## sample length in days
        tradeLength = len(self.tradeFileList)
        quoteLength = len(self.quoteFileList)
        ## total number of trades and quotes
        totalTrades = self.trade_nums
        totalQuotes = self.quote_nums
        ## fraction of trade to quotes
        fracTradeToQuote = totalTrades / totalQuotes
        ## trade returns stats
        dfTrades = pd.concat(self.trade_returns, axis=0).sort_index()
        dfTrades.dropna(inplace=True)
        tradesStat = calculateStats(dfTrades)
        tradesStat += [tradeLength, totalTrades, fracTradeToQuote]
        ### need to be annualized from minute return to annulized return
        ## quote returns stats
        dfQuotes = pd.concat(self.quote_returns, axis=0).sort_index()
        dfQuotes.dropna(inplace = True)
        quotesStat = calculateStats(dfQuotes)
        quotesStat += [quoteLength, totalQuotes, fracTradeToQuote]

        self.tradeStat = tradesStat
        self.quotesStat = quotesStat

        return tradesStat, quotesStat

if __name__ == '__main__':
    workingDir = '/Users/barry/Desktop/NYU Courses/courseSpring2022/algo trading/hw1/DataSet/'
    Ticker = 'APPL'
    taqS = TAQSummary(Ticker, workingDir, freq=10)
    taqS.computeStatForAllDatesWithFreq(ifCleaned=True)
    print(taqS.computeSummary())
    print(taqS.quote_nums)
    print(taqS.trade_nums)
    print(taqS.quote_returns)
    print(taqS.trade_returns)
    