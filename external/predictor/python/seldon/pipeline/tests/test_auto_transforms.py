import unittest
from  .. import auto_transforms as at
import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.externals import joblib

class Test_auto_transforms(unittest.TestCase):

    def test_sklearn_pipeline(self):
        t = at.Auto_transform(ignore_vals=["NA",""])
        transformers = [("auto",t)]
        p = Pipeline(transformers)
        df = pd.DataFrame([True,False])
        df2 = p.fit_transform(df)
        self.assertTrue(df2[0][0] == 1)
        self.assertTrue(df2[0][1] == 0)
        joblib.dump(p,"/tmp/pipeline/p")
        p2 = joblib.load("/tmp/pipeline/p")
        df3 = p2.transform(df)
        self.assertTrue(df3[0][0] == 1)
        self.assertTrue(df3[0][1] == 0)

    def test_bool_col(self):
        df = pd.DataFrame([True,False])
        t = at.Auto_transform(ignore_vals=["NA",""])
        t.fit(df)
        df2 = t.transform(df)
        self.assertTrue(df2[0][0] == 1)
        self.assertTrue(df2[0][1] == 0)

    def test_boolean_col_with_missing(self):
        df = pd.DataFrame([{"a":"true"},{"a":"false"},{"a":""}])
        t = at.Auto_transform(ignore_vals=["NA"])
        t.fit(df)
        df2 = t.transform(df)
        print df2
        self.assertTrue(df2["a"][0] == "true")
        self.assertTrue(df2["a"][1] == "false")
        self.assertTrue(df2["a"][2] == "UKN")

    def test_boolean_col_with_missing2(self):
        df = pd.DataFrame([{"a":"true"},{"a":"false"},{"a":""}])
        t = at.Auto_transform(ignore_vals=["NA"],cat_missing_val="?")
        t.fit(df)
        df2 = t.transform(df)
        print df2
        self.assertTrue(df2["a"][0] == "true")
        self.assertTrue(df2["a"][1] == "false")
        self.assertTrue(df2["a"][2] == "?")

    def test_boolean_col2(self):
        df = pd.DataFrame([{"a":1},{"a":0},{"a":"false"}])
        t = at.Auto_transform(ignore_vals=["NA"])
        t.fit(df)
        df2 = t.transform(df)
        self.assertTrue(df2["a"][0] == 1)
        self.assertTrue(df2["a"][1] == 0)

    def test_change_type_when_ignored_removed(self):
        df = pd.DataFrame([{"a":"NA"},{"a":10},{"a":12},{"a":8}])
        t = at.Auto_transform(ignore_vals=["NA"])
        t.fit(df)
        df2 = t.transform(df)
        self.assertTrue(df2["a"][0] == 0.0)
        self.assertTrue(df2["a"][1] == 0.0)
        self.assertAlmostEqual(df2["a"][2],1.224745,places=4)

    def test_categorical(self):
        df = pd.DataFrame([{"a":""},{"a":"v1"},{"a":"v2"},{"a":"v3"}])
        t = at.Auto_transform(ignore_vals=["NA"])
        t.fit(df)
        df2 = t.transform(df)
        self.assertTrue(df2["a"][0] == "UKN")
        self.assertTrue(df2["a"][1] == "v1")
        self.assertTrue(df2["a"][2] == "v2")


    """
    Test that categorical values can be limited. Those appearing less than some value are removed.
    """
    def test_categorical_values_limit(self):
        df = pd.DataFrame([{"a":10,"b":1},{"a":5,"b":2},{"a":10,"b":3}])
        t = at.Auto_transform(max_values_numeric_categorical=2)
        t.fit(df)
        df2 = t.transform(df)
        self.assertEqual(df["a"][0],"a_10")

    def test_ignored_values(self):
        df = pd.DataFrame([{"a":10},{"a":99},{"a":12},{"a":8}])
        t = at.Auto_transform(ignore_vals=[99])
        t.fit(df)
        df2 = t.transform(df)
        self.assertTrue(df2["a"][0] == 0.0)
        self.assertTrue(df2["a"][1] == 0.0)
        self.assertAlmostEqual(df2["a"][2],1.224745,places=4)


    def test_dates(self):
        df = pd.DataFrame([{"a":"30JAN14:15:11:00","b":"20 Jan 2015"},{"a":"31JAN14:10:11:00","b":"20 Jan 2015"}])
        t = at.Auto_transform(custom_date_formats=["%d%b%y:%H:%M:%S"],date_cols=["a"])
        t.fit(df)
        df2 = t.transform(df)
        self.assertAlmostEqual(df2["a_h1"][0],-0.707,places=2)

        
if __name__ == '__main__':
    unittest.main()

