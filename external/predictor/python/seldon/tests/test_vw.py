import unittest
import pandas as pd
from .. import vw


class Test_vw(unittest.TestCase):

    def test_create_features(self):
        t = vw.VwClassifier(target="target")
        df = pd.DataFrame.from_dict([{"target":"1","b":"c d","c":3},{"target":"2","b":"word2"}])
        t.fit(df)
        scores = t.predict_proba(df)
        print scores
        self.assertEquals(scores.shape[0],2)
        self.assertEquals(scores.shape[1],2)
        
if __name__ == '__main__':
    unittest.main()

