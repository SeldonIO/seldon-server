import unittest
import vw
import pandas as pd



class Test_vw(unittest.TestCase):

    def test_create_features(self):
        t = vw.VwClassifier(target="target")
        df = pd.DataFrame.from_dict([{"target":"1","b":"c d","c":3},{"target":"2","b":"word2"}])
        t.fit(df)
        t.predict_proba(df)
        t.cleanup()
        
if __name__ == '__main__':
    unittest.main()

