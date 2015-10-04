import unittest
import basic_transforms as bt
import pandas as pd

class Test_include_transform(unittest.TestCase):

    def test_include(self):
        t = bt.Include_features_transform(included=["a","b"])
        df = pd.DataFrame.from_dict([{"a":1,"b":2,"c":3}])
        df2 = t.transform(df)
        self.assertTrue(sorted(df2.columns) == sorted(["a","b"]))

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
        df2 = t.transform(df)

class Test_feature_id_transform(unittest.TestCase):

    def test_ids(self):
        df = pd.DataFrame([{"a":"type1"},{"a":"type2"},{"a":"type1"}])
        t = bt.Feature_id_transform(min_size=2,exclude_missing=True)
        t.set_input_feature("a")
        t.set_output_feature("id")
        r = t.fit(df)
        print r
        df2 = t.transform(df)
        print df2
        


if __name__ == '__main__':
    unittest.main()
