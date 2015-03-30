#!/usr/bin/env python

import unittest
import recommender_base
import json

class RecommenderTestCase(unittest.TestCase):
    def test_format_recs(self):
        recs = {1: 0.1, 2: 0.2, 3: 0.3}
        expected = {"recommended": [{"item": 1, "score": 0.1}, {"item": 2, "score": 0.2}, {"item": 3, "score": 0.3}]}
        actual = recommender_base.format_recs( recs )
        self.assertEqual( expected, actual )
    def test_get_data_set(self):
        print "-ok-"
        raw_data =    '[1,2,3,4,5]'
        expected = set([1,2,3,4,5])
        actual = recommender_base.get_data_set(raw_data)
        self.assertEqual( expected, actual )

if __name__ == "__main__":
    #unittest.main()
    suite = unittest.TestLoader().loadTestsFromTestCase(RecommenderTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)

