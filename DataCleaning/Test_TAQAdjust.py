import unittest
from TAQQuotesReader import TAQQuotesReader
from TAQTradesReader import TAQTradesReader
import os
from TAQAdjust import TAQAdjust, filter_sp500
class MyTestCase(unittest.TestCase):
    def test_filter_sp500(self):
        # test if the runner_function filter_sp500 works as expected
        work_path = 'C:/Users/76985/NYU_homework/2022Spring/Algorithm_trading_and_quantitative_strategies/DataSet'
        path_python = os.path.dirname(os.path.abspath('__file__'))

        filter_sp500(work_path)  # make filtration
        os.chdir(path_python)
        os.chdir(work_path + "/quote_SP")
        date_list = os.listdir()
        self.assertEqual(date_list[0],'20070620')
        os.chdir(work_path + "/quote_SP/20070620/")
        date_list = os.listdir()
        self.assertEqual(date_list[0],'A_quotes.binRQ')
        os.chdir(work_path + "/trade_SP")
        date_list = os.listdir()
        self.assertEqual(date_list[0],'20070620')
        os.chdir(work_path + "/trade_SP/20070620")
        date_list = os.listdir()
        self.assertEqual(date_list[0],'A_trades.binRT')
        os.chdir(path_python)

    def test_adj_law(self):
        # test if the make_adj_law performs as expected.
        work_path = 'C:/Users/76985/NYU_homework/2022Spring/Algorithm_trading_and_quantitative_strategies/DataSet'
        path_python = os.path.dirname(os.path.abspath('__file__'))
        TA = TAQAdjust(work_path)  # declare TAQAdjust
        TA.make_adj_law()

        law = TA.get_adj_law()
        law_false = law[law['law'] == False]
        self.assertEqual(law_false['ticker_id'].values[0], 'TXT')
        self.assertEqual(law_false['ticker_id'].values[1], 'OMC')
        self.assertEqual(law_false['ticker_id'].values[2], 'TYC')
        self.assertEqual(law_false['ticker_id'].values[3], 'MTW')
        self.assertEqual(law_false['ticker_id'].values[4], 'LEN')
        self.assertEqual(law_false['ticker_id'].values[5], 'MS')
        self.assertEqual(law_false['ticker_id'].values[6], 'AGN')
        self.assertEqual(law_false['ticker_id'].values[7], 'GILD')
        self.assertEqual(law_false['ticker_id'].values[8], 'ESRX')
        os.chdir(path_python)
    def test_adj_data(self):
        # test if the make_adj_law performs as expected.
        work_path = 'C:/Users/76985/NYU_homework/2022Spring/Algorithm_trading_and_quantitative_strategies/DataSet'
        path_python = os.path.dirname(os.path.abspath('__file__'))
        TA = TAQAdjust(work_path)  # declare TAQAdjust
        TA.make_adj_law()      # make reference book
        TA.adj_data('20070620','Trade') # update the data of date 20070620

        os.chdir(work_path + "/trade_SP_adj")
        date_list = os.listdir()
        self.assertEqual(date_list[0], '20070620')
        os.chdir(work_path + "/trade_SP_adj/20070620/")
        date_list = os.listdir()
        self.assertEqual(date_list[0], 'A_quotes.binRQ')
        os.chdir(path_python)

if __name__ == '__main__':
    unittest.main()
