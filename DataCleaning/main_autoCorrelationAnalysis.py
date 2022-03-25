from AutoCorrelationAnalysis import AutoCorrelationAnalysis

if __name__ == '__main__':

    filePathName = "C:\\Users\\76985\\NYU_homework\\2022Spring\\Algorithm_trading_and_quantitative_strategies\\DataSet"
    # We test the resampled return with frequency {5s, 10s, 20s, 25s, 60s, 300s} with lag {1, 2, 3, 4, 5}.

    freq_list = [5,10,20,25,60,300]
    lag_list = [1,2,3,4,5]
    date = '20070620'
    ticker = 'YUM'
    ACQ = AutoCorrelationAnalysis(filePathName)
    ACQ.testing_ljungbox(date,ticker,freq_list,lag_list)
    '''If we set 0.1 as the threshold of whether there is a significant autocorrelation. 
    We can see that only frequence: 20s and 60s passed ljungbox test for lag belonging to {1,2,3,4,5}'''
    '''Since we also want to maximize the amount of data we use, 
    it is better to make the resampling frequency as samll as possible. 
    The best frequency we should choose is 20s.'''
    best_freq = 20
    '''Dicky Fuller test for cleaned data and drop nan data directly:'''
    ACQ.testing_dickeyfuller(best_freq,drop = True)
    '''Dicky Fuller test for cleaned data and filling nan data by zero'''
    ACQ.testing_dickeyfuller(best_freq,drop = False)