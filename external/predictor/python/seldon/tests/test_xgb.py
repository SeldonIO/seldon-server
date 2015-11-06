import unittest
import pandas as pd
from .. import xgb
import numpy as np
import sys
import seldon.pipeline.pipelines as pl
from sklearn.pipeline import Pipeline
from sklearn.externals import joblib

class Test_xgb(unittest.TestCase):

    def test_sklearn_pipeline(self):
        t = xgb.XGBoostClassifier(target="target",learning_rate=0.1,silent=0,objective='binary:logistic')
        f1 = {"target":0,"b":1.0,"c":0}
        f2 = {"target":1,"b":0,"c":2.0}
        fs = []
        for i in range (1,50):
            fs.append(f1)
            fs.append(f2)
        print "features=>",fs
        df = pd.DataFrame.from_dict(fs)
        estimators = [("xgb",t)]
        p = Pipeline(estimators)
        p.fit(df)
        preds = p.predict_proba(df)
        print preds
        print "-------------------"
        joblib.dump(p,"/tmp/pipeline/p")
        p2 = joblib.load("/tmp/pipeline/p")
        df3 = p2.predict_proba(df)
        print df3

    def test_create_features(self):
        t = xgb.XGBoostClassifier(target="target",learning_rate=0.1,silent=0,objective='binary:logistic')
        f1 = {"target":0,"b":1.0,"c":0}
        f2 = {"target":1,"b":0,"c":2.0}
        fs = []
        for i in range (1,50):
            fs.append(f1)
            fs.append(f2)
        print "features=>",fs
        df = pd.DataFrame.from_dict(fs)
        t.fit(df)
        scores = t.predict_proba(df)
        print scores.shape
        print "scores->",scores[0]
        preds = t.predict(df)
        print "predictions->",preds[0],preds[1]
        self.assertEquals(preds[0],0)
        self.assertEquals(preds[1],1)


    def test_numpy_input(self):
        t = xgb.XGBoostClassifier(n_estimators=10,learning_rate=0.1,silent=0)
        X = np.random.randn(6,4)
        y = np.array([0,1,1,0,0,1])
        t.fit(X,y)
        scores = t.predict_proba(X)
        print scores

        
if __name__ == '__main__':
    unittest.main()

