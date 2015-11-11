from __future__ import absolute_import
from keras.models import Sequential
from keras.layers.core import Dense, Dropout, Activation
from keras.layers.normalization import BatchNormalization
import unittest
import pandas as pd
from seldon import keras
import numpy as np
import sys
from sklearn.pipeline import Pipeline
from sklearn.externals import joblib
from keras.wrappers.scikit_learn import KerasClassifier

def default_classification_model(input_width,num_classes):
    """Default classification model
    """
    model = Sequential()                         
    print "input width=",input_width
    model.add(Dense(5, init='uniform',input_dim=input_width))
    model.add(Activation('tanh'))
    model.add(Dropout(0.5))

    model.add(Dense(num_classes))
    model.add(Activation('softmax'))

    return model


class Test_keras(unittest.TestCase):

    def test_sklearn_pipeline(self):
        t = keras.KerasClassifier(target="target",verbose=1)
        f1 = {"target":0,"b":1.0,"c":0}
        f2 = {"target":1,"b":0,"c":2.0}
        fs = []
        for i in range (1,50):
            fs.append(f1)
            fs.append(f2)
        print "features=>",fs
        df = pd.DataFrame.from_dict(fs)
        estimators = [("keras",t)]
        p = Pipeline(estimators)
        p.fit(df)
        preds = p.predict_proba(df)
        print preds
        print "-------------------"
        joblib.dump(p,"/tmp/p")
        p2 = joblib.load("/tmp/p")
        df3 = p2.predict_proba(df)
        print "df3"
        print df3



        
if __name__ == '__main__':
    unittest.main()
