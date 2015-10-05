import unittest
import auto_transforms as at
import pandas as pd



class Test_auto_transforms(unittest.TestCase):
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

