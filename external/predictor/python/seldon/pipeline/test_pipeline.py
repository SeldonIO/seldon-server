import unittest
import pipelines as pl
import basic_transforms as bt
import pandas as pd

class Test_transform_json(unittest.TestCase):

    def test_transform_json(self):
        t = bt.Include_features_transform(included=["a","b"])
        df = pd.DataFrame.from_dict([{"a":1,"b":2,"c":3}])
        p = pl.Pipeline()
        p.add(t)
        jres = p.transform_json({"a":1,"b":2,"c":3})
        self.assertEquals(jres["a"],1)
        self.assertEquals(jres["b"],2)
        self.assertFalse("c" in jres)

    def test_save_dataframe(self):
        print "save dataframe test"
        df = pd.DataFrame.from_dict([{"a":1,"b":2,"c":3},{"a":5}])
        p = pl.Pipeline()
        p.save_dataframe(df)


if __name__ == '__main__':
    unittest.main()
