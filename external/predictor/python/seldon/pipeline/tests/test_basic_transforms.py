import unittest
from .. import basic_transforms as bt
import pandas as pd

class Test_split_transform(unittest.TestCase):

    def test_multiple_cols(self):
        t = bt.Split_transform(input_features=["a","b"])
        t.set_output_feature("res")
        df = pd.DataFrame.from_dict([{"a":"a b","b":"c d","c":3},{"a":"word1","b":"word2"}])
        df2 = t.transform(df)
        print df2
        self.assertTrue(len(df2["res"][0]) == 4)

    def test_multiple_cols_with_missing_cols(self):
        t = bt.Split_transform(input_features=["a","b"])
        t.set_output_feature("res")
        df = pd.DataFrame.from_dict([{"a":"a b","c":3},{"b":"word2"}])
        df2 = t.transform(df)
        print df2
        self.assertTrue(len(df2["res"][0]) == 2)
        self.assertTrue(len(df2["res"][1]) == 1)

    def test_multiple_cols_numbers_ignored(self):
        t = bt.Split_transform(input_features=["a","b"],ignore_numbers=True)
        t.set_output_feature("res")
        df = pd.DataFrame.from_dict([{"a":"a b","b":"c 1","c":3}])
        print df
        df2 = t.transform(df)
        print df2
        self.assertTrue(len(df2["res"][0]) == 3)


class Test_include_transform(unittest.TestCase):

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

    def test_svm(self):
        df = pd.DataFrame([{"a":1.2,"b":"word","c":["a","b"],"d":{"a":1}},{"a":3,"c":["b","d"]},{"c":{"k":1,"k2":"word"}}])
        t = bt.Svmlight_transform()
        t.set_output_feature("svm")
        t.fit(df)
        models =t.get_models()
        print models
        df2 = t.transform(df)
        print df2 
        self.assertEquals(df2["svm"][1][5],3.0)

    def test_svm_with_include(self):
        df = pd.DataFrame([{"a":1.2,"b":"word","c":["a","b"],"d":{"a":1}},{"a":3,"c":["b","d"]},{"c":{"k":1,"k2":"word"}}])
        t = bt.Svmlight_transform(included=["a"])
        t.set_output_feature("svm")
        t.fit(df)
        df2 = t.transform(df)
        print df2 
        self.assertEquals(df2["svm"][0][1],1.2)

    def test_svm_with_include_and_categorical(self):
        df = pd.DataFrame([{"a":"word1"},{"a":"word2"},{"a":"word1"}])
        t = bt.Svmlight_transform(included=["a"])
        t.set_output_feature("svm")
        t.fit(df)
        df2 = t.transform(df)
        print df2 
        self.assertEquals(df2["svm"][0][2],1)

    def test_svm_with_exclude(self):
        df = pd.DataFrame([{"a":1.2,"b":"word","c":["a","b"],"d":{"a":1}},{"a":3,"c":["b","d"]},{"c":{"k":1,"k2":"word"}}])
        t = bt.Svmlight_transform(excluded=["b","c","d"])
        t.set_output_feature("svm")
        t.fit(df)
        df2 = t.transform(df)
        print "svm_with_exclude"
        print df2 
        self.assertEquals(df2["svm"][0][1],1.2)


class Test_feature_id_transform(unittest.TestCase):

    def test_ids_exclude_too_few(self):
        df = pd.DataFrame([{"a":"type1"},{"a":"type2"},{"a":"type1"}])
        t = bt.Feature_id_transform(min_size=2,exclude_missing=True)
        t.set_input_feature("a")
        t.set_output_feature("id")
        r = t.fit(df)
        df2 = t.transform(df)
        print "dtype is ",df2["id"].dtype
        print df2
        self.assertEquals(df2.shape[0],2)

    def test_ids(self):
        df = pd.DataFrame([{"a":"type1"},{"a":"type2"},{"a":"type1"}])
        t = bt.Feature_id_transform(min_size=1,exclude_missing=True)
        t.set_input_feature("a")
        t.set_output_feature("id")
        r = t.fit(df)
        df2 = t.transform(df)
        self.assertEquals(df2.shape[0],3)
        self.assertEquals(df2["id"][0],1)

    def test_missing_input_feature(self):
        df = pd.DataFrame([{"a":"type1"},{"a":"type2"},{"a":"type1"}])
        t = bt.Feature_id_transform(min_size=1,exclude_missing=True)
        t.set_input_feature("missing")
        t.set_output_feature("id")
        r = t.fit(df)
        df2 = t.transform(df)
        self.assertEquals(df2.shape[0],3)
        self.assertFalse('missing' in df2)


if __name__ == '__main__':
    unittest.main()
