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
                print df2

#    def test_ignore_vals(self):

        
if __name__ == '__main__':
    unittest.main()

