Algorithm Trading Homework 1

Notice: to run the python script and python unit test, please change the work path to a folder named 
'/DataSet' with child folder as '/trade' or '/quote' that contains the raw data!

----------------------------------------Question1-------------------------------------------------------
To filter the data according to S&P 500.xlsx and make adjustment of data, 
we implement TAQAdjust together with a method of filter_sp500 and delete_file as runner functions.

TAQAdjust contains functions such as:
    make_adj_law: generate a dataframe that contains whether or not all stocks need to be adjusted
    get_adj_law: get the dataframe
    adj_data: rewrite data of one date by adjustment factor.
    adj_data_list: rewrite data of dates in one list by adjustment factor.
    multi_adj_data: generate multi processes to implement data adjustment.
    @Runner_function:
    filter_sp500: filter s&p 500 data from original dataset
    delete_file  delete all data in the path

Test_TAQAdjust: 
    test_filter_sp500: test if the runner_function filter_sp500 works as expected
    test_adj_law: test if the make_adj_law performs as expected.
    test_adj_data: test if we rewrite the data as we want.

For data cleaning, we implement TAQCleaner and Test_TAQCleaner.

TAQCleaner contains functions such as:
    processPrices: process the prices sequence from binary data
    processTimestamps: process timestamps from binary data
    beginCleaning: cleaning data with given parameters
    rewriteToFile: rewrite cleaned data to given path
    plotCleaningQuotesResultGraph: plot side by side cleaning result for quotes
    plotCleaningTradesResultGraph: plot side by side cleaning result for trades

Simply modified the workigDir parameters on the part under the script,
we can see the performance of TAQCleaner with the progress bar.

Test_TAQCleaner:
    generate_fake_data: generate_fake_data and write the data to given folder
    testCleaner: test all the data processing performance and plot the cleaning result

---------------------------------------Question2--------------------------------------------------------------------
For data statistics calcualtion, we implement TAQSummary and Test_TAQSummary

TAQSummary contains functions such as:
    loadTradeData: perpare trade file paths for given Ticker in sample period
    loadQuoteData: perpare quote file paths for given Ticker in sample period
    computeStatWithFreq: compute the return statistics for given path
    computeStatForAllDatesWithFreq: utilize computeStatWithFreq, compute the statistics for all paths
    computeSummary: summary the results from computeStatForAllDatesWithFreq

Simply modified the workigDir parameters on the part under the script,
we can see the performance of TAQCleaner with the progress bar.

Test_TAQSummary:
    generate_fake_data: generate_fake_data and write the data to given folder
    testCleaner: test all the data summary statistics with fake data.

workigDir example:
    TAQSummary: workingDir = '/Users/barry/Desktop/NYU Courses/courseSpring2022/algo trading/hw1/DataSet/'
    TAQCleaner: workingDir = '/Users/barry/Desktop/NYU Courses/courseSpring2022/algo trading/hw1'

Please make sure there is a folder named '/DataSet' with child folder as '/trade_SP_Adj' or '/quote_SP_Adj'

---------------------------------------Question3--------------------------------------------------------------------
For autocorrelation analysis, we implement AutoCorrelationAnalysis class and main_autoCorrelationAnalysis.py script

AutoCorrelationAnalysis contains functions such as:
    computeStatWithFreq: return resampled return that is the same as TAQSummary does
    testing_ljungbox: make Ljungbox test for a given date and ticker.
    testing_dickeyfuller: make dickeyfuller test for a given date and ticker.

main_autoCorrelationAnalysis.py is used to do the autocorrelation analysis based on AutoCorrelationAnalysis.
---------------------------------------Question4--------------------------------------------------------------------

For problem 4:

Portfolio.py is the code of risk and return example downloaded from cvxopt website.
We write the result and analysis into the writeup.

turnover.py is the file to calculate the market portfolio holdings at 2007/06/20 and 2007/09/20. 
It write the resulting holdings into two csv files in the same directory. 
This file also calculated and print the turnover rate in two ways. Details are in the writeup. 

Test_turnover.py is the file to test the three methods defined in turnover.py.