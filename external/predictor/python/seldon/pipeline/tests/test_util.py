import unittest
from .. import util as sutils
from .. import basic_transforms as bt
import pandas as pd
import json
import os.path
from sklearn.pipeline import Pipeline
from sklearn.externals import joblib

class Test_wrapper(unittest.TestCase):

    def _create_test_json(self,fname,as_list=False):
        f = open(fname,"w")
        if as_list:
            f.write("[")
        j = {}
        j["a"] = 1
        j["b"] = 2
        f.write(json.dumps(j,sort_keys=True))
        if as_list:
            f.write(",\n")
        else:
            f.write("\n")
        j = {}
        j["a"] = 3
        j["b"] = 4
        f.write(json.dumps(j,sort_keys=True))
        if as_list:
            f.write("]\n")
        else:
            f.write("\n")
        f.close()

    def test_load_json_folders(self):
        w = sutils.Pipeline_wrapper()
        data_folder = w.get_work_folder()+"/events"
        if not os.path.exists(data_folder):
            os.makedirs(data_folder)
        fname = data_folder+"/"+"test.json"
        self._create_test_json(fname)
        df = w.create_dataframe([data_folder])
        print df

    def test_load_json_file(self):
        w = sutils.Pipeline_wrapper()
        fname = w.get_work_folder()+"/"+"test.json"
        self._create_test_json(fname,as_list=True)
        df = w.create_dataframe(fname)
        print df

    def test_save_dataframe(self):
        w = sutils.Pipeline_wrapper()
        df = pd.DataFrame.from_dict([{"a":"a b","b":"c d","c":3},{"a":"word1","b":"word2"}])
        fname = w.get_work_folder()+"/"+"saved.json"
        w.save_dataframe(df,fname,"csv",csv_index=False)
        df2 = w.create_dataframe(fname,df_format="csv")
        from pandas.util.testing import assert_frame_equal
        assert_frame_equal(df.sort(axis=1), df2.sort(axis=1), check_names=True)

    def test_save_pipeline(self):
        w = sutils.Pipeline_wrapper()
        t = bt.Include_features_transform(included=["a","b"])
        transformers = [("include_features",t)]
        p = Pipeline(transformers)
        df = pd.DataFrame.from_dict([{"a":1,"b":2,"c":3}])
        df2 = p.fit_transform(df)
        self.assertTrue(sorted(df2.columns) == sorted(["a","b"]))
        dest_folder = w.get_work_folder()+"/dest_pipeline"
        w.save_pipeline(p,dest_folder)
        p2 = w.load_pipeline(dest_folder)
        df3 = p2.transform(df)
        self.assertTrue(sorted(df3.columns) == sorted(["a","b"]))

if __name__ == '__main__':
    unittest.main()
