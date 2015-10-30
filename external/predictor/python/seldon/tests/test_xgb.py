import unittest
import pandas as pd
from .. import xgb

class Test_xgb(unittest.TestCase):

    def test_create_features(self):
        t = xgb.XGBoostClassifier(target="target",objective='multi:softprob',num_class=2,eta=0.1,silent=0,booster='gbtree')
        #df = pd.DataFrame.from_dict([{"target":"1","b":"c d","c":3},{"target":"2","b":"word2"}])
        df = pd.DataFrame.from_dict([{"target":0,"b":1.0,"c":2.0},{"target":1,"b":2.0}])
        t.fit(df)
        scores = t.predict_proba(df)
        print scores.shape
        print "scores->",scores

        
if __name__ == '__main__':
    unittest.main()

