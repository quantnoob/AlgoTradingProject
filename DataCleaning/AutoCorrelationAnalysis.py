import pandas as pd
import numpy as np
from matplotlib import pyplot as plt

from TAQTradesReader import TAQTradesReader as tr
from TAQQuotesReader import TAQQuotesReader as qr
import os
import datetime
from statsmodels.stats.diagnostic import acorr_ljungbox
from statsmodels.tsa.stattools import adfuller


class AutoCorrelationAnalysis():
    '''
    This class is used for making auto correlation analysis for a certain ticker in a certain date, we can choose
    resampling frequency and lagging terms of Ljungbox testing as many as we want.
    This class also provides method for DickyFuller testing for a certain resampling frequency we choose.
    '''
    def __init__(self, filePathName):
        self.filePathName = filePathName

    def computeStatWithFreq(self, filePathName, freq, type='trade'):

        X = freq
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
                pd.Timestamp(_tsYearMonthDate + \
                             datetime.timedelta(milliseconds=
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
                _midPrice = 1 / 2 * (dataReader.getAskPrice(i) + \
                                     dataReader.getBidPrice(i))
                _prices.append(_midPrice)
                ## Time-Price DataFrame
        _df = pd.DataFrame({'Prices': _prices, 'Time': _tsList}).set_index('Time')

        ## resample data using the given frequency
        _dfResampled = _df.resample(str(X) + 's').first().pct_change()
        # _returns = _dfResampled['Prices'].dropna().to_list()

        ## return the resampled returns in a list, also the number of trades/quotes with date
        return _dfResampled, dataReader.getN()

    def testing_ljungbox(self, date, ticker, freq_list, lag_list, cleaned=True, plot_fig=True,
                         verbose=True):
        # This method is used to make Ljungbox test for a given date and ticker.
        # @variable: date, ticker should be python string and consistent with the work folder that contains data we need
        # @variable: freq_list: python list that contains frequency we need to resample the trading data return
        # @variable: lag_list: python list that contains lag term we want to use for the Ljungbox testing.
        # @variable: cleaned: python boolean; True if we want to use cleaned data
        # @variable: plot_fig: python boolean; True if we want to make a plotting representation.
        # @variable: verbose: python boolean; True if we want to print the testing results

        # @return: a DataFrame that contains testing results.

        df_result = pd.DataFrame([], columns=['lb_stat', 'lb_pvalue', 'freq', 'lag'])

        if cleaned:
            filePathName = self.filePathName + "\\trade_SP_Adj_cleaned\\" + str(date) + "\\" + str(
                ticker) + "_trades.binRT"
        else:
            filePathName = self.filePathName + "\\trade_SP_Adj\\" + str(date) + "\\" + str(ticker) + "_trades.binRT"

        # make resampling and testing for different frequency and lagging.
        for freq in freq_list:
            dfResampled, sampleSize = self.computeStatWithFreq(filePathName, freq, type='trade')
            dfResampled = dfResampled.fillna(0)
            return_df = acorr_ljungbox(dfResampled, lags=lag_list, return_df=True)
            return_df['lag'] = return_df.index
            return_df['freq'] = [freq] * len(lag_list)
            return_df.reset_index(drop=True)
            df_result = pd.concat([df_result, return_df], axis=0)
        df_result.reset_index(drop=True)

        # print the result of dataframe if we choose verbose as True
        if verbose == True:
            print(df_result)
        if plot_fig == True:
            if len(freq_list) % 2 == 1:
                fig = plt.figure(figsize=(3, 3 * len(freq_list)))
            else:
                fig = plt.figure(figsize=(6, int(3 * len(freq_list) / 2)))
            for i in range(len(freq_list)):
                print(df_result[df_result['freq'] == freq_list[i]]['lb_pvalue'].values)
                if len(freq_list) % 2 == 1:
                    axes = plt.subplot(len(freq_list), 1, i + 1)
                else:
                    axes = plt.subplot(int(len(freq_list) / 2), 2, i + 1)
                axes.plot(lag_list, df_result[df_result['freq'] == freq_list[i]]['lb_pvalue'].values,
                          label='p value of test for freq %i(s)' % int(freq_list[i]))
                axes.set_title('p value of test for freq {}s'.format(int(freq_list[i])))
                # axes.legend()
            plt.savefig('ljungbox.jpg')

        self._ljungbox_result = df_result
        return df_result

    def testing_dickeyfuller(self, date, ticker, freq, drop=False, verbose=True, cleaned=True):
        # This method is used to make dickeyfuller test for a given date and ticker.
        # @variable: date, ticker should be python string and consistent with the work folder that contains data we need
        # @variable: freq: frequency we need to resample the trading data return
        # @variable: drop: python boolean; True if we want to drop nan data in resampling;
        #                  False if we want to fill nan data with zeros
        # @variable: plot_fig: python boolean; True if we want to make a plotting representation.
        # @variable: verbose: python boolean; True if we want to print the testing results
        # @variable: cleaned: python boolean; True if we want to use cleaned data
        # @return: a DataFrame that contains testing results.
        if cleaned:
            filePathName1 = self.filePathName + "\\trade_SP_Adj_cleaned\\" + str(date) + "\\" + str(
                ticker) + "_trades.binRT"
            filePathName2 = self.filePathName + "\\quote_SP_Adj_cleaned\\" + str(date) + "\\" + str(
                ticker) + "_quotes.binRQ"
        else:
            filePathName1 = self.filePathName + "\\trade_SP_Adj\\" + str(date) + "\\" + str(ticker) + "_trades.binRT"
            filePathName2 = self.filePathName + "\\quote_S_AdjP\\" + str(date) + "\\" + str(ticker) + "_quotes.binRQ"

        dfResampled_t, sampleSize_t = self.computeStatWithFreq(filePathName1, freq, type='trade')
        dfResampled_q, sampleSize_q = self.computeStatWithFreq(filePathName2, freq, type='quote')
        # dfResampled_t = dfResampled_t.fillna(0)
        # dfResampled_q = dfResampled_q.fillna(0)
        if drop:
            dfResampled_t = dfResampled_t.dropna()
            dfResampled_q = dfResampled_q.dropna()
        else:
            dfResampled_t = dfResampled_t.fillna(0)
            dfResampled_q = dfResampled_q.fillna(0)
        print(dfResampled_t)
        dftest_t = adfuller(dfResampled_t, autolag='AIC')
        dfoutput_t = pd.DataFrame({'Test Statistic': dftest_t[0], 'p-value': dftest_t[1], '#Lags Used': dftest_t[2]},
                                  index=[0])
        dftest_q = adfuller(dfResampled_q, autolag='AIC')
        dfoutput_q = pd.DataFrame({'Test Statistic': dftest_q[0], 'p-value': dftest_q[1], '#Lags Used': dftest_q[2]},
                                  index=[0])
        if verbose == True:
            print("Dickey Fuller Testing result of trade data")
            print(dfoutput_t)
            print("Dickey Fuller Testing result of quote data")
            print(dfoutput_q)

        self._dickeyfuller_result_t = dfoutput_t
        return dfoutput_t, dfoutput_q


if __name__ == '__main__':
    '''
    filePathName = "C:\\Users\\76985\\NYU_homework\\2022Spring\\Algorithm_trading_and_quantitative_strategies\\DataSet"
    freq_list = [5,10,20,25,60,300] 
    lag_list = [1,2,3,4,5]
    date = '20070620'
    ticker = 'YUM'
    ACQ = AutoCorrelationAnalysis(filePathName)
    ACQ.testing_ljungbox(date,ticker,freq_list,lag_list)
    
    best_freq = 20
    ACQ.testing_dickeyfuller(best_freq,drop = True)
    
    ACQ.testing_dickeyfuller(best_freq,drop = False)
    '''
