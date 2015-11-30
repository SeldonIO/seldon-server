import unittest
from seldon import Recommender,Recommender_wrapper
import sys
import logging

class SimpleRecommender(Recommender):
    
    def __init__(self):
        self.res = [(1,0.8),(2,0.7)]
    
    def recommend(self,user,ids,recent_interactions,client,limit):
        return self.res

    
class Test_util(unittest.TestCase):

    def test_save_load(self):
        sr1 = SimpleRecommender()
        res1 = sr1.recommend(1,None,None,"test",2)
        self.assertEqual(len(res1),2)
        
        rr = Recommender_wrapper()
        rr.save_recommender(sr1,"/tmp/simplerec")

        sr2 = rr.load_recommender("/tmp/simplerec")
        res2 = sr2.recommend(1,None,None,"test",2)
        self.assertEqual(len(res2),2)
        
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    unittest.main()

