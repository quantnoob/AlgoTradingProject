import sys
import os
import datetime
import pandas as pd
import numpy as np
from tabulate import tabulate as tb
from tqdm import tqdm
from sklearn.covariance import empirical_covariance
sys.path.append(os.path.join(os.path.abspath('..'), 'DataCleaning'))
from TAQSummary import TAQSummary
from TAQQuotesReader import TAQQuotesReader
from TAQCleaner import TAQCleaner
from pyRMTmain import clipped, optimalShrinkage


class dataChecker(object):
    def __init__(self, workingDir, ticker):
        self.workingDir = workingDir
        self.ticker = ticker
        self.freq = 5*60
        self.taqS = TAQSummary(ticker, workingDir, freq=self.freq)
    
    def checkFileList(self):
        self.taqS.loadTradeData(ifCleaned=False)
        self.taqS.loadQuoteData(ifCleaned=False)
        return self.taqS.tradeFileList, self.taqS.quoteFileList

    def checkDataCleaningQuote(self, fileName):
        taqC = TAQCleaner(fileName, type='quote')
        taqC.processPrices()
        taqC.processTimestamps()
        taqC.cleaningData()
        taqC.plotCleaningQuotesResultGraph()

class covEstimator(object):
    def __init__(self, workingDir, stockUniverseDir, freq = 5*60):
        self.workingDir = workingDir
        self.freq = freq
        self.stockUniverseDir = stockUniverseDir    
        self.stockUniverse = pd.read_hdf(stockUniverseDir,'sp500_list')
        self.stockUniverse = [i for i in self.stockUniverse if i!='CMCSA']
        self.annulized_multiplier_cov = (16-9.5)*3600/freq*252

    def processReturnsForSingleTicker(self, Ticker):
        # initialize taqsummary written before for processing mid-quote returns with given frequency
        taqS = TAQSummary(Ticker, self.workingDir, freq = self.freq)
        # compute mid-quote returns
        taqS.computeStatForAllDatesWithFreq(ifCleaned=True, progress_bar = False)
        # process returns
        returns = pd.concat(taqS.quote_returns, axis=0).sort_index()
        returns.columns = [taqS.Ticker]
        return returns
        
    def processReturnsForAllTickers(self, ifReRun = False):
        # if not re-run the proccess and the file already exists
        if not ifReRun and os.path.exists('SP500_mid-quote_returns.h5'):
            self.returns = pd.read_hdf('SP500_mid-quote_returns.h5','abc')
        else:
            temp_returns = []
            for ticker in (pbar := tqdm(self.stockUniverse)):
                pbar.set_description("Processing %s" % ticker)
                temp_returns.append(self.processReturnsForSingleTicker(ticker))
            temp_returns = pd.concat(temp_returns, axis=1)
            temp_returns.to_hdf('./SP500_mid-quote_returns.h5','abc')
            self.returns  = temp_returns

    def processReturnsForSpecialCase(self, ifReRun=False):
        if not ifReRun and os.path.exists('SP500_mid-quote_returns_fixed.h5'):
            self.returns = pd.read_hdf('SP500_mid-quote_returns_fixed.h5','abc')
        else:
            # fill java with sunw since it is the same company
            self.returns.loc[:,'JAVA'] = self.returns.loc[:,'JAVA'].fillna(self.returns['SUNW'])
            del self.returns['SUNW']
            # delete MXM because it is not actively traded during our sample period
            del self.returns['MXM']
            self.returns.to_hdf('./SP500_mid-quote_returns_fixed.h5','abc')
    
    def processRollingReturns(self, train, test):
        # to deal with missing values
        # for any equity, if the missing values is greater than 10 percent within the training set
        # we drop this equity from the training and test set
        train.index = [datetime.datetime.combine(i[0],i[1]) for i in train.index]
        test.index = [datetime.datetime.combine(i[0],i[1]) for i in test.index]
        null_pct = pd.isna(train).sum()/len(train)
        null_names = null_pct[null_pct > 0.1].index.to_list()
        #print('drop : {}'.format(null_names))
        train.drop(null_names, axis=1, inplace=True)
        test.drop(null_names, axis=1, inplace=True)

        # fill the missing value with 0, because the missing value occurs
        # when the stock is not actively quoted, or the quotes are filtered out by cleaning process
        # it is reasonable to fill the value with 0
        train.fillna(0.0, inplace=True)
        test.fillna(0.0, inplace=True)
        return  train, test

    def train_test_rolling_split(self, train_window = 12, test_window = 2):
        self.train_set = []
        self.test_set = []
        idx = pd.IndexSlice
        # change dataframe id
        temp_returns = self.returns.copy()
        date_list = pd.Series(temp_returns.index.date).unique().tolist()
        temp_returns.index = [temp_returns.index.date, temp_returns.index.time]
        # rolling sampling
        for i in range(train_window,len(date_list)-test_window+1):
            temp_train, temp_test = self.processRollingReturns(
                                    temp_returns.loc[idx[date_list[i-train_window:i],:],:].copy(),
                                    temp_returns.loc[idx[date_list[i:i+test_window],:],:].copy())
            self.train_set.append(temp_train)
            self.test_set.append(temp_test)

    def empiricalCov(self):
        self._empiricalCovList = [pd.DataFrame(empirical_covariance(i) * self.annulized_multiplier_cov,
                                             index=i.columns, columns=i.columns)\
                                      for i in self.train_set]
        #return self._empiricalCovList

    def clippedCov(self):
        self._clippedCovList = [pd.DataFrame(clipped(i, return_covariance=True) * self.annulized_multiplier_cov,
                                                 index=i.columns, columns=i.columns)\
                                 for i in self.train_set]
        #return self._clippedCovList
    
    def optimalShrinkageCov(self):
        self._optimalShrinkageCovList = [pd.DataFrame(optimalShrinkage(i, return_covariance=True, method='kernel') * self.annulized_multiplier_cov,
                                                         index=i.columns, columns=i.columns)\
                                         for i in self.train_set]
        #return self._optimalShrinkageCovList
    def calculateEstimators(self):
        self.processReturnsForAllTickers()
        self.processReturnsForSpecialCase()
        self.train_test_rolling_split()
        self.empiricalCov()
        self.clippedCov()
        self.optimalShrinkageCov()
    
class performanceAnalysis(object):
    def __init__(self):
        pass
    @staticmethod
    def weightEquation(sigma, g):
        return np.linalg.inv(sigma).dot(g)/(g.T.dot(np.linalg.inv(sigma)).dot(g))

    @staticmethod
    def randomVectorOnUnitSphere(dim):
        v = np.random.multivariate_normal(np.zeros(dim),np.eye(dim))
        return v/np.linalg.norm(v)

    @staticmethod
    def periodReturn(test):
        return ((test+1).cumprod()-1).iloc[-1]

    @staticmethod
    def calculateMinimumVariancePortfolio(cov_lst, test_lst):
        # calculate weight of minimum variance portfolio
        # for each training period
        g_lst = [np.ones(len(cov)) for cov in cov_lst]
        w_lst = [pd.Series(performanceAnalysis.weightEquation(sigma, g), index = sigma.index) for sigma,g in zip(cov_lst, g_lst)]
        return w_lst

    @staticmethod
    def calculateRandomLSPortfolio(cov_lst, test_lst):
        # calculate weight of random Long-short portfolio
        # for each training period
        np.random.seed(42)
        g_lst = [np.sqrt(len(cov)) * performanceAnalysis.randomVectorOnUnitSphere(len(cov)) for cov in cov_lst]
        w_lst = [pd.Series(performanceAnalysis.weightEquation(sigma, g), index = sigma.index) for sigma,g in zip(cov_lst, g_lst)]
        return w_lst
    
    @staticmethod
    def calculateOmniscientPortfolio(cov_lst, test_lst, freq):
        # calculate weight of omniscient portfolio
        annulized_factor = (16-9.5)*3600/freq*252
        g_lst = [performanceAnalysis.periodReturn(test) * np.sqrt(test.shape[1]) for test in test_lst]
        w_lst = [pd.Series(performanceAnalysis.weightEquation(sigma, g), index = sigma.index) for sigma,g in zip(cov_lst, g_lst)]
        return w_lst

    @staticmethod
    def annulizedOutOfSampleVol(w_lst, test_lst, freq, ifRes = False):
        ret_lst = [test.dot(weight) for weight, test in zip(w_lst, test_lst)]
        vol_lst = [np.std(ret_s)*np.sqrt((16-9.5)*3600/freq*252) for ret_s in ret_lst]
        if ifRes:
            return ret_lst
        return np.mean(np.array(vol_lst)), np.std(np.array(vol_lst))
    
def main(workingDir, stockUniverseDir):
    c = covEstimator(workingDir, stockUniverseDir)
    c.calculateEstimators()
    estimator_names = ['emperical','clipped','optimal shrinkage']
    estimator_lst = [c._empiricalCovList, c._clippedCovList, c._optimalShrinkageCovList]
    strategy_names = ['Minimum Variance','Random Long-Short','Omniscient']
    result_dic = {}
    for idx,  est in enumerate(estimator_lst):
        # calculate weight
        mv_w = performanceAnalysis.calculateMinimumVariancePortfolio(est, c.test_set)
        rls_w = performanceAnalysis.calculateRandomLSPortfolio(est, c.test_set)
        omni_w = performanceAnalysis.calculateOmniscientPortfolio(est, c.test_set, 60*5)
        # calculate out of sample volatility
        mv_res = performanceAnalysis.annulizedOutOfSampleVol(mv_w, c.test_set, 60*5)
        rls_res = performanceAnalysis.annulizedOutOfSampleVol(rls_w, c.test_set, 60*5)
        omni_res = performanceAnalysis.annulizedOutOfSampleVol(omni_w, c.test_set, 60*5)
        # results
        result_dic[estimator_names[idx]] = [mv_res, rls_res, omni_res]
    df = pd.DataFrame(result_dic)
    df.index = strategy_names
    print(df)

if __name__ == '__main__':
    # workingDir is where the cleaned data is stored
    workingDir = '/Users/barry/Desktop/NYU Courses/courseSpring2022/algo trading/hw1/DataSet/'
    # stock universe is the list of sp500 stored in a h5 format, hw submission should contain this file
    stockUniverseDir = '/Users/barry/Desktop/NYU Courses/courseSpring2022/algo trading/AlgoTradingProjects/AlgoTradingProject/CovarianceMatrix/sp500_list.h5' 
    main(workingDir, stockUniverseDir)
