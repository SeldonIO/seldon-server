import unittest
import tfidf_transform as tf
import pandas as pd



class Test_tfidf_transform(unittest.TestCase):

    def test_list_input_feature(self):
                df = pd.DataFrame([{"a":[111111,222222,333333]},{"a":[333333,444444,555555]}])
                t = tf.Tfidf_transform()
                t.set_input_feature("a")
                t.set_output_feature("tfidf")
                t.fit(df)
                df2 = t.transform(df)
                print df2

        
if __name__ == '__main__':
    unittest.main()

