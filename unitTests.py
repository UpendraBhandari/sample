'''
Created on 27-Dec-2016

@author: uppi
'''
import unittest
import RecipeMainDF


class Test(unittest.TestCase):

     
    # negative test    
    def test_case1(self):
        self.assertEqual(RecipeMainDF.parseTime('PT10M','PT10M'),'Hard')
        
    def test_case2(self):
        self.assertEqual(RecipeMainDF.parseTime('PT1H10M','PT10M'),'Hard')
        
    def test_case3(self):
        self.assertEqual(RecipeMainDF.parseTime('PT0H10M','PT10M'),'Easy')
        
    def test_case4(self):
        self.assertEqual(RecipeMainDF.parseTime('PT30M','PT30M'),'Medium')
        
    def test_case5(self):
        self.assertEqual(RecipeMainDF.parseTime('PT0D0H40M','PT10M'),'Medium')
        
    


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()