import unittest
import select_features_transform as sf
import pandas as pd
import numpy as np


class Test_select_feature_transform(unittest.TestCase):

    def test_remove_novariance_col(self):
        df = pd.DataFrame([{"a":10,"b":1},{"a":10,"b":2},{"a":10,"b":2}])
        t = sf.Select_features_transform(use_variance=True)
        t.fit(df)
        df2 = t.transform(df)
        self.assertEqual(len(df2.columns),1)

    def test_remove_cat_min_col(self):
        df = pd.DataFrame([{"a":"b"},{"a":"b"},{"a":"c"}])
        t = sf.Select_features_transform(min_cat_percent=0.5)
        t.fit(df)
        df2 = t.transform(df)
        

        
if __name__ == '__main__':
    unittest.main()

