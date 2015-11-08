import unittest
import pandas as pd
from .. import vw
import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.externals import joblib

class Test_vw(unittest.TestCase):

    def test_sklearn_pipeline(self):
        t = vw.VWClassifier(target="target")
        f1 = {"target":0,"b":1.0,"c":0}
        f2 = {"target":1,"b":0,"c":2.0}
        fs = []
        for i in range (1,50):
            fs.append(f1)
            fs.append(f2)
        print "features=>",fs
        df = pd.DataFrame.from_dict(fs)
        estimators = [("vw",t)]
        p = Pipeline(estimators)
        print "fitting"
        p.fit(df)
        print "get preds 1 "
        preds = p.predict_proba(df)
        print preds
        print "-------------------"
        t.close()
        joblib.dump(p,"/tmp/pipeline/p")
        p2 = joblib.load("/tmp/pipeline/p")
        print "get preds 2"
        df3 = p2.predict_proba(df)
        print df3
        vw2 = p2._final_estimator
        vw2.close()


    def test_zero_based_target(self):
        t = vw.VWClassifier(target="target",target_readable="name")
        try:
            df = pd.DataFrame.from_dict([{"target":0,"b":"c d","c":3,"name":"zeroTarget"},{"target":1,"b":"word2","name":"oneTarget"}])
            t.fit(df)
            scores = t.predict_proba(df)
            print "scores->",scores
            self.assertEquals(scores.shape[0],2)
            self.assertEquals(scores.shape[1],2)
            idMap = t.get_class_id_map()
            print idMap
            formatted_recs_list=[]
            for index, proba in enumerate(scores[0]):
                print index,proba
                if index in idMap:
                    indexName = idMap[index]
                else:
                    indexName = str(index)
                formatted_recs_list.append({
                        "prediction": str(proba),
                        "predictedClass": indexName,
                        "confidence" : str(proba)
        })
            print formatted_recs_list
        finally:
            t.close()


    def test_create_features(self):
        t = vw.VWClassifier(target="target")
        try:
            df = pd.DataFrame.from_dict([{"target":"1","b":"c d","c":3},{"target":"2","b":"word2"}])
            t.fit(df)
            scores = t.predict_proba(df)
            print scores
            self.assertEquals(scores.shape[0],2)
            self.assertEquals(scores.shape[1],2)
        finally:
            t.close()

    def test_predictions(self):
        try:
            t = vw.VWClassifier(target="target")
            df = pd.DataFrame.from_dict([{"target":"1","b":"c d","c":3},{"target":"2","b":"word2"}])
            t.fit(df)
            preds = t.predict(df)
            print preds
            self.assertEquals(preds[0],0)
            self.assertEquals(preds[1],1)
        finally:
            t.close()


    def test_dict_feature(self):
        try:
            t = vw.VWClassifier(target="target")
            df = pd.DataFrame.from_dict([{"target":"1","df":{"1":0.234,"2":0.1}},{"target":"2","df":{"1":0.5}}])
            t.fit(df)
            scores = t.predict_proba(df)
            print scores
            self.assertEquals(scores.shape[0],2)
            self.assertEquals(scores.shape[1],2)
        finally:
            t.close()

    def test_list_feature(self):
        try:
            t = vw.VWClassifier(target="target",num_iterations=10)
            df = pd.DataFrame.from_dict([{"target":"1","df":["a","b","c","d"]},{"target":"2","df":["x","y","z"]}])
            t.fit(df)
            df2 = pd.DataFrame.from_dict([{"df":["a","b","c","d"]},{"df":["x","y","z"]}])
            scores = t.predict_proba(df2)
            if not scores is  None:
                print scores
            self.assertEquals(scores.shape[0],2)
            self.assertTrue(scores[0][0]>scores[0][1])
            self.assertEquals(scores.shape[1],2)
            self.assertTrue(scores[1][0]<scores[1][1])
        finally:
            t.close()


    def test_vw_same_score_bug(self):
        try:
            t = vw.VWClassifier(target="target",num_iterations=10)
            df = pd.DataFrame.from_dict([{"target":"1","df":["a","b","c","d"]},{"target":"2","df":["x","y","z"]}])
            t.fit(df)
            df2 = pd.DataFrame.from_dict([{"df":["a","b","c","d"]},{"df":["x","y","z"]}])
            scores = t.predict_proba(df2)
            score_00 = scores[0][0]
            score_10 = scores[1][0]
            for i in range(1,4):
                scores = t.predict_proba(df2)
                self.assertEquals(scores[0][0],score_00)
                self.assertEquals(scores[1][0],score_10)
        finally:
            t.close()

    def test_large_number_features(self):
        try:
            t = vw.VWClassifier(target="target")
            f = {}
            f2 = {}
            for i in range(1,5000):
                f[i] = 1
                f2[i] = 0.1
            df = pd.DataFrame.from_dict([{"target":"1","df":f},{"target":"2","df":f2}])
            t.fit(df)
            scores = t.predict_proba(df)
            print scores
            self.assertEquals(scores.shape[0],2)
            self.assertEquals(scores.shape[1],2)
        finally:
            t.close()


    def test_numpy_input(self):
        try:
            t = vw.VWClassifier()
            X = np.random.randn(6,4)
            y = np.array([1,2,1,1,2,2])
            t.fit(X,y)
            scores = t.predict_proba(X)
            print scores
        finally:
            t.close()

        
if __name__ == '__main__':
    unittest.main()

