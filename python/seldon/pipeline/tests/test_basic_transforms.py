import unittest
from .. import basic_transforms as bt
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.externals import joblib
import logging


class Test_binary_transform(unittest.TestCase):

    def test_sklearn_pipeline(self):
        df = pd.DataFrame.from_dict([{"a":"something"},{}])
        t = bt.Binary_transform(input_feature="a",output_feature="abin")
        transformers = [("binary_transform",t)]
        p = Pipeline(transformers)
        df2 = p.fit_transform(df)
        self.assertEquals(df["abin"][0],1)
        self.assertEquals(df["abin"][1],0)

    def test_sklearn_pipeline_numbers(self):
        df = pd.DataFrame.from_dict([{"a":2},{"a":0}])
        t = bt.Binary_transform(input_feature="a",output_feature="abin")
        transformers = [("binary_transform",t)]
        p = Pipeline(transformers)
        df2 = p.fit_transform(df)
        print df2
        self.assertEquals(df["abin"][0],1)
        self.assertEquals(df["abin"][1],0)

    def test_sklearn_pipeline_str_numbers(self):
        df = pd.DataFrame.from_dict([{"a":"2"},{"a":"0"}])
        t = bt.Binary_transform(input_feature="a",output_feature="abin")
        transformers = [("binary_transform",t)]
        p = Pipeline(transformers)
        df2 = p.fit_transform(df)
        print df2
        self.assertEquals(df["abin"][0],1)
        self.assertEquals(df["abin"][1],0)


class Test_exclude_transform(unittest.TestCase):

    def test_sklearn_pipeline(self):
        df = pd.DataFrame.from_dict([{"a":"something","b":1},{"a":"something2"}])
        t = bt.Exclude_features_transform(excluded=["b"])
        transformers = [("exclude_transform",t)]
        p = Pipeline(transformers)
        df2 = p.fit_transform(df)
        self.assertEquals(len(df2.columns),1)


class Test_split_transform(unittest.TestCase):


    def test_multiple_cols(self):
        t = bt.Split_transform(input_features=["a","b"],output_feature="res")
        df = pd.DataFrame.from_dict([{"a":"a b","b":"c d","c":3},{"a":"word1","b":"word2"}])
        transformers = [("split_transform",t)]
        p = Pipeline(transformers)
        df2 = p.transform(df)
        self.assertTrue(len(df2["res"][0]) == 4)

    def test_multiple_cols_with_missing_cols(self):
        t = bt.Split_transform(input_features=["a","b"],output_feature="res")
        df = pd.DataFrame.from_dict([{"a":"a b","c":3},{"b":"word2"}])
        transformers = [("split_transform",t)]
        p = Pipeline(transformers)
        df2 = p.transform(df)
        self.assertTrue(len(df2["res"][0]) == 2)
        self.assertTrue(len(df2["res"][1]) == 1)

    def test_multiple_cols_numbers_ignored(self):
        t = bt.Split_transform(input_features=["a","b"],ignore_numbers=True,output_feature="res")
        df = pd.DataFrame.from_dict([{"a":"a b","b":"c 1","c":3}])
        transformers = [("split_transform",t)]
        p = Pipeline(transformers)
        df2 = p.transform(df)
        self.assertTrue(len(df2["res"][0]) == 3)


class Test_include_transform(unittest.TestCase):

    def test_sklearn_pipeline(self):
        t = bt.Include_features_transform(included=["a","b"])
        transformers = [("include_features",t)]
        p = Pipeline(transformers)
        df = pd.DataFrame.from_dict([{"a":1,"b":2,"c":3}])
        df2 = p.fit_transform(df)
        self.assertTrue(sorted(df2.columns) == sorted(["a","b"]))
        joblib.dump(p,"/tmp/p")
        p2 = joblib.load("/tmp/p")
        df3 = p2.transform(df)
        self.assertTrue(sorted(df3.columns) == sorted(["a","b"]))

    def test_include(self):
        t = bt.Include_features_transform(included=["a","b"])
        df = pd.DataFrame.from_dict([{"a":1,"b":2,"c":3}])
        df2 = t.transform(df)
        self.assertTrue(sorted(df2.columns) == sorted(["a","b"]))

    def test_include_missing_column(self):
        t = bt.Include_features_transform(included=["a","b"])
        df = pd.DataFrame.from_dict([{"a":1,"c":3}])
        df2 = t.transform(df)
        self.assertTrue(sorted(df2.columns) == sorted(["a"]))

class Test_exist_features_transform(unittest.TestCase):

    def test_exist(self):
        t = bt.Exist_features_transform(included=["b"])
        df = pd.DataFrame.from_dict([{"a":1,"b":2},{"a":3}])
        df2 = t.transform(df)
        self.assertTrue(df.shape[0] == 1)

class Test_svmlight_transform(unittest.TestCase):

    def test_svm_with_list(self):
        df = pd.DataFrame([{"a":{"abc":1,"def":2}},{"a":{"gh1":1,"def":2}}])
        t = bt.Svmlight_transform(included=["a"],output_feature="svm")
        t.fit(df)
        df2 = t.transform(df)
        print df2
        self.assertEquals(df2["svm"][0][0],(1,1))

    def test_svm_with_unicode(self):
        df = pd.DataFrame([{"a":{u"abc":1,u"£def":2}},{"a":{"gh1":1,"def":2}}])
        t = bt.Svmlight_transform(included=["a"],output_feature="svm")
        t.fit(df)
        df2 = t.transform(df)
        print df2
        self.assertEquals(df2["svm"][0][0],(1,1))


    def test_svm(self):
        df = pd.DataFrame([{"a":1.2,"b":"word","c":["a","b"],"d":{"a":1}},{"a":3,"c":["b","d"]},{"c":{"k":1,"k2":"word"}}])
        t = bt.Svmlight_transform(output_feature="svm")
        t.fit(df)
        df2 = t.transform(df)
        print df2
        self.assertEquals(df2["svm"][0][1],(5,1.2))

    def test_svm_with_include(self):
        df = pd.DataFrame([{"a":1.2,"b":"word","c":["a","b"],"d":{"a":1}},{"a":3,"c":["b","d"]},{"c":{"k":1,"k2":"word"}}])
        t = bt.Svmlight_transform(included=["a"],output_feature="svm")
        t.fit(df)
        df2 = t.transform(df)
        self.assertEquals(df2["svm"][0][0],(1,1.2))

    def test_svm_with_include_and_categorical(self):
        df = pd.DataFrame([{"a":"word1"},{"a":"word2"},{"a":"word1"}])
        t = bt.Svmlight_transform(included=["a"],output_feature="svm")
        t.fit(df)
        df2 = t.transform(df)
        self.assertEquals(df2["svm"][0][0],(2,1))

    def test_svm_with_exclude(self):
        df = pd.DataFrame([{"a":1.2,"b":"word","c":["a","b"],"d":{"a":1}},{"a":3,"c":["b","d"]},{"c":{"k":1,"k2":"word"}}])
        t = bt.Svmlight_transform(excluded=["b","c","d"],output_feature="svm")
        t.fit(df)
        df2 = t.transform(df)
        self.assertEquals(df2["svm"][0][0],(1,1.2))


class Test_feature_id_transform(unittest.TestCase):

    def test_ids_max_classes(self):
        df = pd.DataFrame([{"a":"type1"},{"a":"type2"},{"a":"type1"}])
        t = bt.Feature_id_transform(max_classes=1,exclude_missing=True,input_feature="a",output_feature="id")
        r = t.fit(df)
        df2 = t.transform(df)
        self.assertEquals(df2.shape[0],2)

    def test_ids_max_classes_zero_based(self):
        df = pd.DataFrame([{"a":"type1"},{"a":"type2"},{"a":"type1"}])
        t = bt.Feature_id_transform(max_classes=1,exclude_missing=True,input_feature="a",output_feature="id",zero_based=True)
        r = t.fit(df)
        df2 = t.transform(df)
        self.assertEquals(df2.shape[0],2)



    def test_ids_exclude_too_few(self):
        df = pd.DataFrame([{"a":"type1"},{"a":"type2"},{"a":"type1"}])
        t = bt.Feature_id_transform(min_size=2,exclude_missing=True,input_feature="a",output_feature="id")
        r = t.fit(df)
        df2 = t.transform(df)
        self.assertEquals(df2.shape[0],2)

    def test_ids(self):
        df = pd.DataFrame([{"a":"type1"},{"a":"type2"},{"a":"type1"}])
        t = bt.Feature_id_transform(min_size=1,exclude_missing=True,input_feature="a",output_feature="id")
        r = t.fit(df)
        df2 = t.transform(df)
        self.assertEquals(df2.shape[0],3)
        self.assertEquals(df2["id"][0],1)

    def test_missing_input_feature(self):
        df = pd.DataFrame([{"a":"type1"},{"a":"type2"},{"a":"type1"}])
        t = bt.Feature_id_transform(min_size=1,exclude_missing=True,input_feature="a",output_feature="id")
        r = t.fit(df)
        df2 = t.transform(df)
        self.assertEquals(df2.shape[0],3)
        self.assertFalse('missing' in df2)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    unittest.main()
