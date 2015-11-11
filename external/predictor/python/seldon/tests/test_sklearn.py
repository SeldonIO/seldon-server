import unittest
import pandas as pd
from .. import sklearn_estimator as ske
import numpy as np
import sys
from sklearn.pipeline import Pipeline
from sklearn.externals import joblib
from sklearn.linear_model import LogisticRegression

class Test_keras(unittest.TestCase):

    def test_sklearn_pipeline(self):
        t = ske.SKLearnClassifier(clf=LogisticRegression(),target="target")
        f1 = {"target":0,"b":1.0,"c":0}
        f2 = {"target":1,"b":0,"c":2.0}
        fs = []
        for i in range (1,50):
            fs.append(f1)
            fs.append(f2)
        df = pd.DataFrame.from_dict(fs)
        estimators = [("lr",t)]
        p = Pipeline(estimators)
        p.fit(df)
        preds = p.predict_proba(df)
        preds2 = p.predict(df)
        print preds2
        print preds
        print "-------------------"
        joblib.dump(p,"/tmp/p")
        p2 = joblib.load("/tmp/p")
        df3 = p2.predict_proba(df)
        print "df3"
        print df3



        
if __name__ == '__main__':
    unittest.main()
