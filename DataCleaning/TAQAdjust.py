import shutil
import time
import sys
import numpy as np
import os
from TAQQuotesReader import TAQQuotesReader
from TAQTradesReader import TAQTradesReader
import pandas as pd
import shutil


class TAQAdjust(object):

    # This class uses excel files of S&p 500 to make price adjustment of quotes data and trade data in the support of
    # TAQQuotesReader and TAQTradesReader which have been updated to make the format of I/O consistent.

    # @method: make_adj_law - generate a dataframe that contains whether or not all stocks need to be adjusted
    # according to the excel. If the adjust factor changed, the price and size data need to be adjusted and rewrite
    # data into disc. If doesn't, we only need to copy the same data into a new work path.
    # @method: get_adj_law - return the adj_law dataframe
    # @method: adj_data - rewrite data of one date by adjustment factor.
    # @method: adj_data_list - rewrite data of dates in one list by adjust factor.
    # @method: multi_adj_data - generate multi processes to implement data adjustment.

    def __init__(self, work_path):
        # @variable: work_path - path that contains data we want to make adjustment
        # filePathName_Read
        columns = 'B,H,BA,BB'  # BA adj_p, BB adj_s

        adj_sp500 = pd.read_excel("s&p500.xlsx", usecols=columns, names=['dates', 'ticker_id', 'adj_s', 'adj_p'])
        adj_sp500 = adj_sp500.fillna(1)
        self._adj_data = adj_sp500
        self._adj_law = None
        self._work_path = work_path

    def make_adj_law(self):
        # generate a dataframe that contains whether or not all stocks need to be adjusted
        # according to the excel. If the adjust factor changed, the price and size data need to be adjusted and rewrite
        # data into disc. If doesn't, we only need to copy the same data into a new work path.
        ticker_id = self._adj_data['ticker_id'].unique()

        adj_s_mean_list = np.array(
            list(map(lambda ticker: self._adj_data['adj_s'][self._adj_data['ticker_id'] == ticker].values.mean()
                     , ticker_id)))
        adj_p_mean_list = np.array(
            list(map(lambda ticker: self._adj_data['adj_p'][self._adj_data['ticker_id'] == ticker].values.mean()
                     , ticker_id)))
        adj_s_initial_list = np.array(
            list(map(lambda ticker: self._adj_data['adj_s'][self._adj_data['ticker_id'] == ticker].values[0]
                     , ticker_id)))
        adj_p_initial_list = np.array(
            list(map(lambda ticker: self._adj_data['adj_p'][self._adj_data['ticker_id'] == ticker].values[0]
                     , ticker_id)))
        adj_s_law_list = np.isclose(adj_s_mean_list, adj_s_initial_list)
        adj_p_law_list = np.isclose(adj_p_mean_list, adj_p_initial_list)
        self._adj_law = pd.DataFrame({'ticker_id': ticker_id, 'law': np.logical_and(adj_s_law_list, adj_p_law_list)})

    def get_adj_law(self):
        return self._adj_law

    def adj_data(self, date, DataType):
        # rewrite data of one date by adjustment factor.

        # @variable: date - the date we want to adjust data
        # @variable: DataType - "Quote" or "Trade"

        path_python = os.path.dirname(os.path.abspath('__file__'))
        date_f = float(date)

        if self._adj_law is not None:
            work_path = self._work_path
            if DataType == 'Trade':
                read_path = work_path + "/trade_SP"
                write_path = work_path + "/trade_SP_Adj"

            if DataType == 'Quote':
                read_path = work_path + "/quote_SP"
                write_path = work_path + "/quote_SP_Adj"

            if not os.path.exists(write_path):
                try:
                    os.makedirs(write_path)
                except OSError as e:
                    print(f"An error has occurred: {e}")
                    raise

            os.chdir(read_path + "/" + date)
            stocks_this_date = os.listdir()

            file_write_path_date = write_path + "/" + date

            if not os.path.exists(file_write_path_date):
                try:
                    os.makedirs(file_write_path_date)
                except OSError as e:
                    print(f"An error has occurred: {e}")
                    raise

            nStocks = len(stocks_this_date)
            for iStock, stocks_str in enumerate(stocks_this_date):
                ticker_id = stocks_str[:-13]
                file_read_path = read_path + "/" + date + "/" + stocks_str
                file_write_path = write_path + "/" + date + "/" + stocks_str
                if self._adj_law[self._adj_law['ticker_id'] == ticker_id]['law'].values[0]:
                    shutil.copy(file_read_path, file_write_path)
                else:
                    if DataType == 'Trade':
                        Reader = TAQTradesReader(file_read_path)
                    else:
                        Reader = TAQQuotesReader(file_read_path)
                    try:
                        adj_s = self._adj_data['adj_s'][
                            (self._adj_data['dates'] == date_f) & (
                                    self._adj_data['ticker_id'] == ticker_id)].values[0]
                        adj_p = self._adj_data['adj_p'][
                            (self._adj_data['dates'] == date_f) & (
                                    self._adj_data['ticker_id'] == ticker_id)].values[0]
                    except IndexError as e:
                        adj_s = 1
                        adj_p = 1

                    Reader.rewrite_adj(file_write_path, adj_s, adj_p)
                sys.stdout.write("\r updating process:{0}/%d (%.2f%%)".format(iStock + 1) % (
                    nStocks, (iStock + 1) * 100.0 / nStocks))
                sys.stdout.flush()

        os.chdir(path_python)

    def adj_data_list(self, date_list, DataType):
        # rewrite data of dates in one list by adjustment factor.

        # @variable: date_list - python list that contains dates we want to adjust data
        # @variable: DataType - "Quote" or "Trade"
        path_python = os.path.dirname(os.path.abspath('__file__'))
        nDates = len(date_list)
        for iDate, date in enumerate(date_list):
            date_f = float(date)

            if self._adj_law is not None:
                work_path = self._work_path
                if DataType == 'Trade':
                    read_path = work_path + "/trade_SP"
                    write_path = work_path + "/trade_SP_Adj"

                if DataType == 'Quote':
                    read_path = work_path + "/quote_SP"
                    write_path = work_path + "/quote_SP_Adj"

                if not os.path.exists(write_path):
                    try:
                        os.makedirs(write_path)
                    except OSError as e:
                        print(f"An error has occurred: {e}")
                        raise

                os.chdir(read_path + "/" + date)
                stocks_this_date = os.listdir()
                # if DataType == 'Trade':
                #     stocks_this_date = ['HON_trades.binRT']
                # if DataType == 'Quote':
                #     stocks_this_date = ['HON_quotes.binRQ']
                file_write_path_date = write_path + "/" + date

                if not os.path.exists(file_write_path_date):
                    try:
                        os.makedirs(file_write_path_date)
                    except OSError as e:
                        print(f"An error has occurred: {e}")
                        raise

                # nStocks = len(stocks_this_date)
                for iStock, stocks_str in enumerate(stocks_this_date):
                    ticker_id = stocks_str[:-13]
                    file_read_path = read_path + "/" + date + "/" + stocks_str
                    file_write_path = write_path + "/" + date + "/" + stocks_str
                    if self._adj_law[self._adj_law['ticker_id'] == ticker_id]['law'].values[0]:
                        shutil.copy(file_read_path, file_write_path)

                    else:
                        if DataType == 'Trade':
                            Reader = TAQTradesReader(file_read_path)
                        else:
                            Reader = TAQQuotesReader(file_read_path)
                        try:
                            adj_s = self._adj_data['adj_s'][
                                (self._adj_data['dates'] == date_f) & (
                                        self._adj_data['ticker_id'] == ticker_id)].values[0]
                            adj_p = self._adj_data['adj_p'][
                                (self._adj_data['dates'] == date_f) & (
                                        self._adj_data['ticker_id'] == ticker_id)].values[0]
                        except IndexError as e:
                            adj_s = 1
                            adj_p = 1
                        Reader.rewrite_adj(file_write_path, adj_s, adj_p)

            sys.stdout.write("\r updating process:{0}/%d (%.2f%%)".format(iDate + 1) % (
                nDates, (iDate + 1) * 100.0 / nDates))
            sys.stdout.flush()
        os.chdir(path_python)

    def multi_adj_data(self, nThreads):
        # generate multi processes to implement data adjustment.

        # @variable: nThreads - the number of threads we want to generate.

        import multiprocessing
        from math import ceil
        t1 = time.time()
        self.make_adj_law()

        path_python = os.path.dirname(os.path.abspath('__file__'))
        #################################################################################
        ##########################    Update Trade Data #################################
        #################################################################################

        DataType = 'Trade'
        work_path = self._work_path
        read_path = work_path + "/trade_SP"
        os.chdir(read_path)
        date_list = os.listdir()

        print('Start adjusting trade data')
        nDates = len(date_list)
        oneListLen = int(ceil(nDates * 1.0 / nThreads))

        adjLists = []
        oneList = []
        for iDate, date in enumerate(date_list):
            if (iDate > 0 and (iDate + 1) % oneListLen == 0) or iDate == nDates - 1:
                oneList.append(date)
                adjLists.append(oneList)
                oneList = []
            else:
                oneList.append(date)

        threads = []

        for adjDates in adjLists:
            threads.append(multiprocessing.Process(target=self.adj_data_list, args=(adjDates, DataType)))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        t2 = time.time()
        print("\n all data updated, using time: %.2fs" % (t2 - t1))

        #################################################################################
        ##########################    Update Quote Data #################################
        #################################################################################

        DataType = 'Quote'
        work_path = self._work_path
        read_path = work_path + "/quote_SP"
        os.chdir(read_path)
        date_list = os.listdir()

        print('Start adjusting quote data')
        nDates = len(date_list)
        oneListLen = int(ceil(nDates * 1.0 / nThreads))

        adjLists = []
        oneList = []
        for iDate, date in enumerate(date_list):
            if (iDate > 0 and (iDate + 1) % oneListLen == 0) or iDate == nDates - 1:
                oneList.append(date)
                adjLists.append(oneList)
                oneList = []
            else:
                oneList.append(date)

        threads = []

        for adjDates in adjLists:
            threads.append(multiprocessing.Process(target=self.adj_data_list, args=(adjDates, DataType)))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        t2 = time.time()
        os.chdir(path_python)
        print("\n all data updated, using time: %.2fs" % (t2 - t1))


## Runner function
def filter_sp500(_work_path):
    # this function is designed for filter s&p 500 data from original dataset
    sp500_ticker = pd.read_excel("s&p500.xlsx", usecols='B,H', names=['dates', 'tickers'])
    sp500_ticker = sp500_ticker.dropna()

    path_python = os.path.dirname(os.path.abspath('__file__'))
    # work path should contain 4 folders: quote with all quotes data / trade with all trades data /
    # quote_SP to get moved SP quotes data / trade_SP to get moved SP trades data
    t1 = time.time()
    print("Start filtering sp500 stocks")
    os.chdir(_work_path)
    DataSet_listdir = os.listdir()
    print(DataSet_listdir)

    # make the os work path to folder ./quote
    os.chdir(_work_path + "/quote")

    # get all folders named by dates
    date_folds_listdir = os.listdir()

    for date in date_folds_listdir:
        if date == '.DS_Store':
            continue
        date_folds_path = _work_path + "/quote/" + date
        date_folds_SP_path = _work_path + "/quote_SP/" + date

        # create date folds in the folder ./quote_SP
        if not os.path.exists(date_folds_SP_path):
            try:
                os.makedirs(date_folds_SP_path)
            except OSError as e:
                print(f"An error has occurred: {e}")
                raise

        ticker_array = sp500_ticker['tickers'][sp500_ticker['dates'] == float(date)].values
        # move SP quotes from date folders in ./quote to date folders in ./quote_SP
        for ticker in ticker_array:
            original_pos = date_folds_path + "/" + ticker + "_quotes.binRQ"
            target_pos = date_folds_SP_path + "/" + ticker + "_quotes.binRQ"
            if os.path.exists(original_pos):
                try:
                    shutil.copy(original_pos, target_pos)
                except FileNotFoundError as e:
                    print(f"An error has occurred: {e}")
                    raise

    # similar procedure for trades data
    os.chdir(_work_path + "/trade")
    date_folds_listdir = os.listdir()
    for date in date_folds_listdir:
        if date == '.DS_Store':
            continue
        date_folds_path = _work_path + "/trade/" + date
        date_folds_SP_path = _work_path + "/trade_SP/" + date
        if not os.path.exists(date_folds_SP_path):
            try:
                os.makedirs(date_folds_SP_path)
            except OSError as e:
                print(f"An error has occurred: {e}")
                raise

        ticker_array = sp500_ticker['tickers'][sp500_ticker['dates'] == float(date)].values
        for ticker in ticker_array:
            original_pos = date_folds_path + "/" + ticker + "_trades.binRT"
            target_pos = date_folds_SP_path + "/" + ticker + "_trades.binRT"
            if os.path.exists(original_pos):
                try:
                    shutil.copy(original_pos, target_pos)
                except FileNotFoundError as e:
                    print(f"An error has occurred: {e}")
                    raise
    t2 = time.time()
    os.chdir(path_python)
    print("Filtering finished with time: ", t2 - t1)


def delete_file(path):
    # this method is designed for deleting all tata in the path
    path_python = os.path.dirname(os.path.abspath('__file__'))
    os.chdir(path)
    folds_listdir = os.listdir()
    for fold in folds_listdir:
        os.chdir(path + '/' + fold)
        files_listdir = os.listdir()
        for file in files_listdir:
            os.remove(path + '/' + fold + '/' + file)
    os.chdir(path_python)


if __name__ == '__main__':
    # work_path = "C:/Users/76985/NYU_homework/2022Spring/Algorithm_trading_and_quantitative_strategies/DataSet"
    work_path = '/Users/barry/Desktop/NYU Courses/courseSpring2022/algo trading/hw1/DataSet'
    path_python = os.path.dirname(os.path.abspath('__file__'))
    filter_sp500(work_path)  # make filtration
    # delete_file(work_path+"/quote_SP") # delete files with errors
    # delete_file(work_path+"/trade_SP")
    # delete_file(work_path+"/quote_SP_Adj")
    # delete_file(work_path+"/trade_SP_Adj")
    os.chdir(path_python)
    TA = TAQAdjust(work_path)  # 声明TAQAdjust
    # TA.make_adj_law()      # 制作各股票是否adj的reference book
    # TA.adj_data('20070620','Trade') # 只更新一天的trade数据
    TA.multi_adj_data(nThreads=8)  # 全部更新完（以内含make_adj_law和adj_data）
    law = TA.get_adj_law()
    print("Stocks we need to adjust are: ")
    print(law[law['law'] == False])

