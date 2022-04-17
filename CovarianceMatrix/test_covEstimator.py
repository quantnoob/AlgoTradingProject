import unittest
import pandas as pd
import numpy as np
import datetime
import gzip
import struct
import os
import sys
from tqdm import tqdm
from sklearn.covariance import empirical_covariance
from covEstimator import covEstimator
from covEstimator import performanceAnalysis
from TAQSummary import TAQSummary
from TAQQuotesReader import TAQQuotesReader
from TAQCleaner import TAQCleaner
from pyRMTmain import clipped, optimalShrinkage


class test_covEstimator(unittest.TestCase):

    def testEstimator(self):
        workingDir = '/Users/barry/Desktop/NYU Courses/courseSpring2022/algo trading/hw1/DataSet/'
        stockUniverseDir = '/Users/barry/Desktop/NYU Courses/courseSpring2022/algo trading/AlgoTradingProjects/AlgoTradingProject/CovarianceMatrix/sp500_list.h5'
    
        c = covEstimator(workingDir, stockUniverseDir, freq=5*60)
        c.processReturnsForAllTickers()
        c.processReturnsForSpecialCase()
        # we test the successful dealing with special cases:
        # delete the XMX stock as well as merge JAVA and SUNW
        self.assertTrue('SUNW' not in c.returns, "need tomerge JAVA and SUNW")
        self.assertTrue('MXM' not in c.returns, "need to delete MXM stock")

        # test train test rolling split
        c.train_test_rolling_split()
        self.assertEqual(c.train_set[0].shape[0], 12*78)
        self.assertEqual(c.test_set[0].shape[0], 2*78)
        self.assertEqual(len(c.test_set), len(c.train_set))
        self.assertEqual(len(c.test_set), 52)
        
        # test filling missing values and drop not active quoted stocks
        self.assertEqual(pd.isna(c.train_set[0]).sum().sum(),0)

        # test covariance estimation
        
        c.empiricalCov()
        c.clippedCov()
        c.optimalShrinkageCov()
        self.assertEqual(c._clippedCovList[0].shape[0], 502)
        # show the covestimation results
        
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
        