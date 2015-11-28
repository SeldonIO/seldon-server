import unittest
from .. import cross_validation as cf
import seldon.xgb as xgb
import pandas as pd
import logging


class Test_kfolds(unittest.TestCase):

    def test_kfold(self):
        x = xgb.XGBoostClassifier(target="target",learning_rate=0.1,silent=0,objective='binary:logistic')
        t = cf.Seldon_KFold(x,3)
        f1 = {"target":0,"b":1.0,"c":0}
        f2 = {"target":1,"b":0,"c":2.0}
        fs = []
        for i in range (1,50):
            fs.append(f1)
            fs.append(f2)
        print "features=>",fs
        df = pd.DataFrame.from_dict(fs)
        t.fit(df)


        
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    unittest.main()

