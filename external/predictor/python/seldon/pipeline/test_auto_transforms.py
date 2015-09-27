import unittest
import auto_transforms as at
import json

class Categorical_data():

    def __init__(self):
        self.data = '{"a":"A"}\n{"a":"A"}\n{"a":"B"}'

    def __iter__(self):
        for line in self.data.split("\n"):
            j = json.loads(line)
            yield j



class Test_auto_transforms(unittest.TestCase):
"""
     Test that categorical values can be limited. Those appearing less than some value are removed.
"""
    def test_categorical_values_limit(self):
        atrans = at.Auto_transform(min_categorical_keep_feature=0.5)
        d = Categorical_data()
        atrans.fit(d)
        for j in d:
            jNew = atrans.transform(j)
            self.assertTrue(not "b" in jNew)

if __name__ == '__main__':
    unittest.main()

