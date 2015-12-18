import unittest
from seldon.text import Tag_Recommender, DefaultJsonCorpus
import logging


class Test_docsim(unittest.TestCase):

    def get_docs(self):
        return [{"id":1,"text":"","tags":"football,soccer"},
                {"id":2,"text":"","tags":"money,finance"},
                {"id":3,"text":"","tags":"football"},
                {"id":4,"text":"","tags":"money"}]


    def test_jaccard(self):
        corpus = DefaultJsonCorpus(self.get_docs())
        ds = Tag_Recommender(max_s2_size=1.0,min_s2_size=0)
        ds.fit(corpus)
        res = ds.knn("football",k=1,metric='jaccard')
        self.assertEqual(res[0],("soccer",0.5))

    def test_jaccard_max_constraint(self):
        corpus = DefaultJsonCorpus(self.get_docs())
        ds = Tag_Recommender(max_s2_size=0.1,min_s2_size=0)
        ds.fit(corpus)
        res = ds.knn("football",k=1,metric='jaccard')
        self.assertEqual(len(res),0)

    def test_asym(self):
        corpus = DefaultJsonCorpus(self.get_docs())
        ds = Tag_Recommender(max_s2_size=1.0,min_s2_size=0)
        ds.fit(corpus)
        res = ds.knn("football",k=1,metric='asym')
        self.assertEqual(res[0],("soccer",1.0))

    def test_asym_min_constraint(self):
        corpus = DefaultJsonCorpus(self.get_docs())
        ds = Tag_Recommender(max_s2_size=1.0,min_s2_size=10)
        ds.fit(corpus)
        res = ds.knn("football",k=1,metric='asym')
        self.assertEqual(len(res),0)


        
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    unittest.main()
