import unittest
import basic_transforms as bt
import pandas as pd

class Test_include_transform(unittest.TestCase):

    def test_include(self):
        t = bt.Include_features_transform(included=["a","b"])
        df = pd.DataFrame.from_dict([{"a":1,"b":2,"c":3}])
        df2 = t.transform(df)
        self.assertTrue(sorted(df2.columns) == sorted(["a","b"]))


if __name__ == '__main__':
    unittest.main()
