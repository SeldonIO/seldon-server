import unittest
from .. import sklearn_transform as ssk
import pandas as pd
import json
import os.path
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import LabelBinarizer

class Test_sklearn(unittest.TestCase):


    def test_standard_scaler(self):
        df = pd.DataFrame.from_dict([{"a":1,"b":2},{"a":2,"b":3}])
        t = ssk.sklearn_transform(input_features=["a"],output_features=["a_scaled"],transformer=StandardScaler())
        t.fit(df)
        df_2 = t.transform(df)
        print df_2
        self.assertEquals(df_2["a_scaled"][0],-1)
        self.assertEquals(df_2["a_scaled"][1],1)
        
    def test_label_binarizer(self):
        df = pd.DataFrame.from_dict([{"a":"a"},{"a":"b"},{"a":"c"}])
        t = ssk.sklearn_transform(input_features=["a"],output_features_prefix="a",transformer=LabelBinarizer())
        t.fit(df)
        df_2 = t.transform(df)
        self.assertEquals(df_2["a_0"][0],1)


if __name__ == '__main__':
    unittest.main()
