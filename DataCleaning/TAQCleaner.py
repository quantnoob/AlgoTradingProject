from TAQTradesReader import TAQTradesReader as tr
from TAQQuotesReader import TAQQuotesReader as qr
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

import pandas as pd
import numpy as np
import datetime
import platform
import os
import time
import sys

class TAQCleaner(object):
    def __init__(self, filePathName, type=None):
        self._filePathName = filePathName
        ## dataReader
        if type == 'trade':
            self.dataReader = tr(filePathName)
        elif type =='quote':
            self.dataReader = qr(filePathName)
        else:
            raise ValueError('Data type is invalid')
        ## data type
        self.type = type
        ## prices lst
        self.prices = None
        self.bidPrices = None
        self.askPrices = None
        ## outlier timestamp lst
        self.outlierIdx = None
        ## timestamp lst
        self.tsList = None
        self.rawTradesDF = None
        self.rawQuotesDF = None
        self.cleanedTradesDF = None
        self.cleanedQuotesDF = None

    ## function to read in all the prices, for quote and trade type
    ## if type is quote, the self.prices is mid price
    def processPrices(self):
        if self.prices is None and self.type == 'trade':
            self.prices = []
            for i in range(self.dataReader.getN()):
                ## add trade price
                self.prices.append(self.dataReader.getPrice(i))   

        if self.prices is None and self.type == 'quote':
            self.prices = []
            self.bidPrices = []
            self.askPrices = []
            for i in range(self.dataReader.getN()):
                ## add bid
                self.bidPrices.append(self.dataReader.getBidPrice(i))
                ## add ask
                self.askPrices.append(self.dataReader.getAskPrice(i))
                ## add mid price
                _midPrice = 1/2 * (self.dataReader.getAskPrice(i) +\
                            self.dataReader.getBidPrice(i))
                self.prices.append(_midPrice)                
        return self.prices

    ## read in all the timestamps and convert it to pandas timestamp
    def processTimestamps(self):
        self._tsYearMonthDate = datetime.datetime.fromtimestamp(
                self.dataReader.getSecsFromEpocToMidn()
        )
        if self.tsList is None:
            self.tsList = []
            for i in range(self.dataReader.getN()):
                self.tsList.append(
                    ## Y-M-D + m-s
                    pd.Timestamp(self._tsYearMonthDate+\
                        datetime.timedelta(milliseconds = 
                            self.dataReader.getMillisFromMidn(i)))
                )
    ## cleaning data based on given parameters
    def cleaningData(self, K=21, gamma_multiplier = 0.00005):
        if self.prices is None:
            raise ValueError('prices need to be processed first.')

        half_window = int((K-1)/2)
        self.outlierIdx = []
        for i in range(self.dataReader.getN()):
            ## at the beginning of the day
            if i<half_window:
                left_bound = 0
                right_bound = K
            ## at the end of the day
            elif self.dataReader.getN() - i < 3:
                left_bound = self.dataReader.getN()-K
                right_bound = self.dataReader.getN()
            ## normal trading timestamp
            else:
                left_bound = i-half_window
                right_bound = i+half_window+1
            ## calculate neighbor std and mean
            _std = np.std(self.prices[left_bound:right_bound])
            _mean = np.mean(self.prices[left_bound:right_bound])
            ## compare with current price
            ## detect outlier
            if abs(self.prices[i] - _mean) > 1.5*_std +\
                    gamma_multiplier * _mean:
                self.outlierIdx.append(i)

    def getRawTradesDataFrame(self):
        if self.type=='quote':
            raise ValueError('Trades not available')
        if self.prices is None or self.tsList is None:
            self.processPrices()
            self.processTimestamps()
        self.rawTradesDF = pd.DataFrame({'Price':self.prices,
                        'Time':self.tsList})
        return self.rawTradesDF

    def getCleanedTradesDataFrame(self):
        if self.type=='quote':
            raise ValueError('Trades not available')

        if self.prices is None or self.tsList is None:
            self.processPrices()
            self.processTimestamps()
        if self.outlierIdx is None:
            raise ValueError('Plese clean the data first.')

        self.cleanedPrices = [self.prices[i] for i in range(self.dataReader.getN())\
            if i not in self.outlierIdx]
        self.cleanedTimeStamps = [self.tsList[i] for i in range(self.dataReader.getN())\
            if i not in self.outlierIdx]

        self.cleanedTradesDF = pd.DataFrame({'Price':self.cleanedPrices,
                        'Time':self.cleanedTimeStamps})
        return self.cleanedTradesDF

    def getRawQuotesDataFrame(self):
        if self.type=='trade':
            raise ValueError('Quotes not available')

        if self.prices is None or self.tsList is None:
            self.processPrices()
            self.processTimestamps()
        self.rawQuotesDF = pd.DataFrame({'Ask':self.askPrices,
                        'Bid':self.bidPrices,
                        'Mid':self.prices,
                        'Time':self.tsList})
        return self.rawQuotesDF

    def getCleanedQuotesDataFrame(self):
        if self.type=='trade':
            raise ValueError('Quotes not available')
            
        if self.prices is None or self.tsList is None:
            self.processPrices()
            self.processTimestamps()
        if self.outlierIdx is None:
            raise ValueError('Plese clean the data first.')

        self.cleanedPrices = [self.prices[i] for i in range(self.dataReader.getN())\
            if i not in self.outlierIdx]
        self.cleanedBidPrices = [self.bidPrices[i] for i in range(self.dataReader.getN())\
            if i not in self.outlierIdx]
        self.cleanedAskPrices = [self.askPrices[i] for i in range(self.dataReader.getN())\
            if i not in self.outlierIdx]
        self.cleanedTimeStamps = [self.tsList[i] for i in range(self.dataReader.getN())\
            if i not in self.outlierIdx]

        self.cleanedQuotesDF = pd.DataFrame({'Ask':self.cleanedAskPrices,
                        'Bid':self.cleanedBidPrices,
                        'Mid':self.cleanedPrices,
                        'Time':self.cleanedTimeStamps})
        return self.cleanedQuotesDF

    ## plot the data cleaning result
    def plotCleaningTradesResultGraph(self, startTS=None, endTS=None, filePath=None, markersize = 5):
        if self.type=='quote':
            raise ValueError('Trades not available')

        if not startTS:
            startTS = self.tsList[0]
        if not endTS:
            endTS = self.tsList[-1]

        if self.rawTradesDF is None:
            self.getRawTradesDataFrame()
        if self.cleanedTradesDF is None:
            self.getCleanedTradesDataFrame()

        rawData = self.rawTradesDF[np.logical_and(self.rawTradesDF['Time']>=startTS,
                                    self.rawTradesDF['Time']<=endTS)]
        rawData.set_index('Time',inplace=True)

        cleanedData = self.cleanedTradesDF[np.logical_and(self.cleanedTradesDF['Time']>=startTS,
                                    self.cleanedTradesDF['Time']<=endTS)]
        cleanedData.set_index('Time',inplace=True)
      
        outliersTS = [self.tsList[i] for i in self.outlierIdx]
        fig, ax = plt.subplots(2,1,figsize=(20,12),sharex=True)
        ax[0].scatter(rawData.index, rawData['Price'],s=markersize,color='deepskyblue')
        ax[0].legend(['Raw Trades Data'])
        ax[0].set_title('Before cleaning')
        ax[1].scatter(cleanedData.index, cleanedData['Price'],s=markersize, color='deepskyblue')
        ax[1].legend(['Cleaned Trades Data'])
        ax[1].set_title('After cleaning')
        for x in outliersTS:
            if x in rawData.index:
                ax[0].axvline(x, color='r',ls=':',lw='0.5')
        myFmt = mdates.DateFormatter('%H:%M:%S') # here you can format your datetick labels as desired
        plt.gca().xaxis.set_major_formatter(myFmt)
        fig.suptitle('Trades Data Cleaning Comparasion')
        plt.show()
        #if filePath is not None:
        #    plt.savefig(os.path.join(filePath,'cleaningComparasion_quotes'), format='jpg')

    ## plot the data cleaning result
    def plotCleaningQuotesResultGraph(self, startTS=None, endTS=None, filePath=None, markersize = 5):
        if self.type=='trade':
            raise ValueError('Quotes not available')
        if not startTS:
            startTS = self.tsList[0]
        if not endTS:
            endTS = self.tsList[-1]

        if self.rawQuotesDF is None:
            self.getRawQuotesDataFrame()

        if self.cleanedQuotesDF is None:
            self.getCleanedQuotesDataFrame()

        rawData = self.rawQuotesDF[np.logical_and(self.rawQuotesDF['Time']>=startTS,
                                    self.rawQuotesDF['Time']<=endTS)]
        rawData.set_index('Time',inplace=True)

        cleanedData = self.cleanedQuotesDF[np.logical_and(self.cleanedQuotesDF['Time']>=startTS,
                                    self.cleanedQuotesDF['Time']<=endTS)]
        cleanedData.set_index('Time',inplace=True)

        outliersTS = [self.tsList[i] for i in self.outlierIdx]

        fig, ax = plt.subplots(2,1,figsize=(20,12),sharex=True)
        ax[0].step(rawData.index, rawData['Ask'], '*--',color='deepskyblue', linewidth=1, markersize=markersize, where='mid')
        ax[0].step(rawData.index, rawData['Bid'], '*--',color='deepskyblue', linewidth=1, markersize=markersize,where='mid')
        ax[0].scatter(rawData.index, rawData['Mid'],s=markersize, color='r')
        ax[0].legend(['Raw Ask','Raw Bid','Raw Mid'])
        ax[0].set_title('Before cleaning')
        ax[1].step(cleanedData.index, cleanedData['Ask'], '*--',color='deepskyblue',linewidth=1, markersize=markersize, where='mid')
        ax[1].step(cleanedData.index, cleanedData['Bid'], '*--',color='deepskyblue',linewidth=1, markersize=markersize,where='mid')
        ax[1].scatter(cleanedData.index, cleanedData['Mid'],s=markersize,color='r')
        ax[1].legend(['Cleaned Ask','Cleaned Bid','Cleaned Mid'])
        ax[1].set_title('After cleaning')
        for x in outliersTS:
            if x in rawData.index:
                ax[0].axvline(x, color='r',ls=':',lw='0.5')
        myFmt = mdates.DateFormatter('%H:%M:%S') # here you can format your datetick labels as desired
        plt.gca().xaxis.set_major_formatter(myFmt)
        fig.suptitle('Quotes Data Cleaning Comparasion')
        plt.show()
        #if filePath is not None:
        #    plt.savefig(os.path.join(filePath,'/cleaningComparasion_quotes'), format='jpg')

    def getPrices(self):
        if self.prices is None:
            self.processPrices()
        return self.prices

    def getBidPrices(self):
        if self.type=='trade':
            raise ValueError('Quotes not available')
        else:
            return self.bidPrices

    def getAskPrices(self):
        if self.type=='trade':
            raise ValueError('Quotes not available')
        else:
            return self.askPrices

    def getTimestamps(self):
        if self.tsList is None:
            self.processTimestamps()
        return self.tsList

    def getOutLierPercent(self):
        return len(self.outlierIdx)/len(self.prices)

    ## rewrite the cleaned file to given filePathName
    def rewriteToFile(self, filePathName):
        if self.type == 'quote':
            self.getCleanedQuotesDataFrame()
            ts = []
            bidSize = []
            askSize = []
            for i in range(self.dataReader.getN()):
                if i not in self.outlierIdx:
                    ts.append(self.dataReader.getMillisFromMidn(i))
                    bidSize.append(self.dataReader.getBidSize(i))
                    askSize.append(self.dataReader.getAskSize(i))
            self.dataReader.rewriteCleaned(ts,
                            bidSize, self.cleanedBidPrices,
                             askSize,self.cleanedAskPrices,filePathName)
        elif self.type == 'trade':
            self.getCleanedTradesDataFrame()
            ts = []
            sizes = []

            for i in range(self.dataReader.getN()):
                if i not in self.outlierIdx:
                    ts.append(self.dataReader.getMillisFromMidn(i))
                    sizes.append(self.dataReader.getSize(i))
    
            self.dataReader.rewriteCleaned(ts, self.cleanedPrices,sizes,
                                            filePathName)
        else:
            raise ValueError('data type is invalid')

## Function tools to utilize TAQCleaner 
def beginCleaning(path, K=21, gamma_multiplier=0.00005):
    start = time.time()
    readingPath = path[0]
    writingPath = path[1]
    if 'trade' in readingPath:
        type='trade'
    elif 'quote' in readingPath:
        type='quote'
    else:
        raise ValueError('Path name incorrect.')
    ## cleaning data   
    cleaner = TAQCleaner(readingPath, type)
    cleaner.processPrices()
    cleaner.processTimestamps()
    cleaner.cleaningData(K=K, gamma_multiplier=gamma_multiplier)
    #print('Cleaning consume {}s'.format(time.time()-start))
    start2 = time.time()
    ## write cleaned data to file
    parentfolder = os.path.dirname(writingPath)

    if not os.path.exists(parentfolder):
        os.makedirs(parentfolder)
    cleaner.rewriteToFile(writingPath)
    #print('Writing consumes {}s'.format(time.time()-start2))
    
def parallel_computing(f, params_list, n_cores=10):
    p = mp.ProcessPool(n_cores)
    chunksize = int(len(params_list) / n_cores)
    p.map(f, params_list, chunksize=chunksize)

## perform data cleaning with given parameters
## we can choose for which ticker we want to clean
## if target_Ticker is None, perform cleaning on all Tickers, which takes hours to complete
def main(workingDir, K=21, gamma_multiplier=0.00005, target_Ticker = None):
    adj_suffix = '_SP_Adj'
    type = 'trade'
    tradeDir = os.path.join(workingDir,'Dataset',type+adj_suffix)
 
    tradeDateList = [i for i in os.listdir(tradeDir) if os.path.isdir(
                                                            os.path.join(tradeDir, i)
                                                             )]
    trade_params = []
    tradeDateList.sort()
    for _date in tradeDateList:
        tickerOnGivenDate = os.listdir(os.path.join(tradeDir,_date))
        ## create a reading and writing dir for beginCleaning function to perform cleaning
        for _ticker in tickerOnGivenDate:
            trade_params.append(
                [os.path.join(tradeDir,_date,_ticker),
                os.path.join(tradeDir+'_cleaned',_date,_ticker)] 
            )

    type = 'quote'
    quoteDir = os.path.join(workingDir ,'Dataset'\
                    , type + adj_suffix)
    quoteDateList = [i for i in os.listdir(quoteDir) if os.path.isdir( os.path.join(quoteDir,i) )]
    quote_params = []
    quoteDateList.sort()
    for _date in quoteDateList:
        tickerOnGivenDate = os.listdir( os.path.join(quoteDir,_date) )
        ## create a reading and writing dir for beginCleaning function to perform cleaning
        for _ticker in tickerOnGivenDate:
            quote_params.append( 
                [os.path.join(quoteDir,_date,_ticker),
                os.path.join(quoteDir+'_cleaned',_date,_ticker)]
            )
        
    
    from tqdm import tqdm
    import multiprocessing as mp
    n_core = 10
    pool1 = mp.Pool(n_core)
    pool2 = mp.Pool(n_core)
    
    ## if target_Ticker is specified
    ## only clean the data for target Ticker
    if target_Ticker is not None:
        trade_params = [i for i in trade_params if target_Ticker in i[0]]
        quote_params = [j for j in quote_params if target_Ticker in j[0]]

    print(trade_params[0])
    pbar1 = tqdm(total = len(trade_params))
    pbar1.set_description('trade cleaning')
    update1 = lambda *args : pbar1.update()

    pbar2 = tqdm(total = len(quote_params))
    pbar2.set_description('quote cleaning')
    update2 = lambda *args : pbar2.update()
    
    for _param1,_param2 in zip(trade_params,quote_params):
        pool1.apply_async(beginCleaning,(_param1,K,gamma_multiplier),callback=update1)
        pool2.apply_async(beginCleaning, (_param2,K,gamma_multiplier),callback=update2)
    pool1.close()
    pool1.join()
    pool2.close()
    pool2.join()

if __name__ == '__main__':
    workingDir = '/Users/barry/Desktop/NYU Courses/courseSpring2022/algo trading/hw1'
    start = time.time()
    targetTicker = None
    main(workingDir, K=21, gamma_multiplier=0.00005, target_Ticker = targetTicker)
    print('Finished all, time consumed {}'.format(time.time()-start))