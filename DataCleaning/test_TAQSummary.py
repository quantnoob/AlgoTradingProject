import unittest
from TAQSummary import TAQSummary
import gzip
import struct
import os

def generate_fake_data(workingDir,type = None, ifCleaned = True):
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
        if ifCleaned:
            tradeFilePath = os.path.join(workingDir,'trade_SP_Adj_cleaned','20070620')
            tradeDataPath = os.path.join(tradeFilePath,'FAKE_trades.binRT')
        else:
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
        if ifCleaned:
            quoteFilePath = os.path.join(workingDir,'quote_SP_Adj_cleaned','20070620')
            quoteDataPath = os.path.join(quoteFilePath,'FAKE_quotes.binRQ')
        else:
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

class Test_TAQSummary(unittest.TestCase):
    def fake_data_generate(self):
        workingDir = '/Users/barry/Desktop/NYU Courses/courseSpring2022/algo trading/hw1/FakeData'
        dirSuffix = '/DataSet'
        workingDir = workingDir + dirSuffix
        try:
            generate_fake_data(workingDir,type='trade',ifCleaned=False)
            generate_fake_data(workingDir,type='trade',ifCleaned=True)
        except ValueError:
            print('Wrong with fake trade data generate')
        try:
            generate_fake_data(workingDir,type='quote',ifCleaned=False)
            generate_fake_data(workingDir,type='quote',ifCleaned=True)
        except ValueError:
            print('Wrong with fake quote data generate')

    def testSummary(self):
        self.fake_data_generate()
        workingDir = '/Users/barry/Desktop/NYU Courses/courseSpring2022/algo trading/hw1/FakeData'
        dirSuffix = '/DataSet/'
        workingDir = workingDir + dirSuffix

        taqSummary1 = TAQSummary('FAKE',workingDir,freq=1)
    
        taqSummary1.computeStatForAllDatesWithFreq(ifCleaned=True)
        taqSummary1.computeSummary()

        taqSummary2 = TAQSummary('FAKE',workingDir,freq=1)
        taqSummary2.computeStatForAllDatesWithFreq(ifCleaned=False)
        taqSummary2.computeSummary()
        ## test firt six summary statistics
        cleanTradeStat = [146977887.56756756, 0.0, 383195.242836573, 286901425.62162167,
                             6.324545314968749, 39.999913577228874, 0.999998997996996,]
        cleanQuoteStat = [146977887.56756756, 0.0, 383195.242836573, 286901425.62162167,
                             6.324545314968749, 39.999913577228874, 0.999998997996996]
        dirtyTradeStat = [146977887.56756756, 0.0, 383195.242836573, 286901425.62162167,
                             6.324545314968749, 39.999913577228874, 0.999998997996996]
        dirtyQuoteStat = [146977887.56756756, 0.0, 383195.242836573, 286901425.62162167,
                             6.324545314968749, 39.999913577228874, 0.999998997996996]
        
        testCleanTradeStat = taqSummary1.tradeStat[:6]
        testCleanQuoteStat = taqSummary1.quotesStat[:6]
        testDirtyTradeStat = taqSummary2.tradeStat[:6]
        testDirtyQuoteStat = taqSummary2.quotesStat[:6]

        for i in range(6):
            self.assertAlmostEquals(cleanTradeStat[i],testCleanTradeStat[i],5)
            self.assertAlmostEquals(cleanQuoteStat[i],testCleanQuoteStat[i],5)
            self.assertAlmostEquals(dirtyTradeStat[i],testDirtyTradeStat[i],5)
            self.assertAlmostEquals(dirtyQuoteStat[i],testDirtyQuoteStat[i],5)
        


if __name__ == '__main__':
    workingDir = '/Users/barry/Desktop/NYU Courses/courseSpring2022/algo trading/hw1/FakeData'
    dirSuffix = '/DataSet/'
    workingDir = workingDir + dirSuffix
    try:
        generate_fake_data(workingDir,type='trade',ifCleaned=False)
        generate_fake_data(workingDir,type='trade',ifCleaned=True)
    except ValueError:
        print('Wrong with fake trade data generate')
    try:
        generate_fake_data(workingDir,type='quote',ifCleaned=False)
        generate_fake_data(workingDir,type='quote',ifCleaned=True)
    except ValueError:
        print('Wrong with fake quote data generate')


    taqSummary1 = TAQSummary('FAKE',workingDir,freq=1)
    
    taqSummary1.computeStatForAllDatesWithFreq(ifCleaned=True)
    taqSummary1.computeSummary()

    taqSummary2 = TAQSummary('FAKE',workingDir,freq=1)
    taqSummary2.computeStatForAllDatesWithFreq(ifCleaned=False)
    taqSummary2.computeSummary()

    print('='*50)
    print(taqSummary1.tradeStat)
    print('='*50)
    print(taqSummary1.quotesStat)
    print('='*50)
    print(taqSummary2.tradeStat)
    print('='*50)
    print(taqSummary2.quotesStat)
    print('='*50)
