#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 28 21:53:14 2022

@author: Haoyang Li
"""

import unittest
import turnover
import pandas as pd

class Test_turnover(unittest.TestCase):

    def test_weight_calc(self):
        d = {'Price': [50, 30, 20], 
        'Shares': [6,10,20]
       }
        df = pd.DataFrame(data=d)
        result = turnover.weight_calc(df['Price'],df['Shares'])

        self.assertEqual( result[0], 0.3 )
        self.assertEqual( result[1], 0.3 )
        self.assertEqual( result[2], 0.4 )
        
    def test_create_date_table(self):
        d = {'Date': [20070620, 20070920, 20070620, 20070720], 
        'ID': [1,4,2,3],
        'Price': [50,30,20,40],
        'Shares': [6,10,20,30]
       }
        df = pd.DataFrame(data=d)
        result = turnover.create_date_table(df)
        
        self.assertEqual( result[0]['ID'][0], 1)
        self.assertEqual( result[0]['ID'][2], 2)
        self.assertEqual( result[1]['ID'][3], 3)
        self.assertEqual( result[2]['ID'][1], 4)
        
    def test_turnover_calc(self):
        
        #situation where both stocks are changed
        d1 = {'Ticker': ['AAPL', 'TSLA'], 
        'Price': [40,60],
        'Weight': [0.4,0.6]
       }
        d2 = {'Ticker': ['AAPL', 'TSLA'], 
        'Price': [50,50],
        'Weight': [0.3,0.7]
       }
        df1 = pd.DataFrame(data=d1)
        df2 = pd.DataFrame(data=d2)
        buy,sell = turnover.turnover_calc(df1,df2)
        
        self.assertEqual( buy, 0.2)
        self.assertEqual( sell, -0.2)
        
        #situations where a stock is added / removed from the index
        d3 = {'Ticker': ['AAPL'], 
        'Price': [40],
        'Weight': [0.4]
       }
        d4 = {'Ticker': ['TSLA'], 
        'Price': [50],
        'Weight': [0.7]
       }
        df3 = pd.DataFrame(data=d3)
        df4 = pd.DataFrame(data=d4)
        buy1,sell1 = turnover.turnover_calc(df3,df4)
        
        self.assertEqual( buy1, 0.7)
        self.assertEqual( sell1, -0.4)
        
        #situation where both stocks are unchanged
        d5 = {'Ticker': ['AAPL', 'TSLA'], 
        'Price': [40,60],
        'Weight': [0.4,0.6]
       }
        df5 = pd.DataFrame(data=d5)
        buy2,sell2 = turnover.turnover_calc(df5,df5)
        
        self.assertEqual( buy2, 0)
        self.assertEqual( sell2, 0)

if __name__ == "__main__":
    unittest.main()
