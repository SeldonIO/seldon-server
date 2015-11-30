import unittest
from seldon.text import DocumentSimilarity, DefaultJsonCorpus
import logging
from gensim import interfaces, utils
from gensim.corpora.dictionary import Dictionary
import copy


class Test_docsim(unittest.TestCase):

    def get_docs(self):
        return [{"id":1,"text":"an article about sports and football, Arsenel, Liverpool","tags":"football"},
                {"id":2,"text":"an article about football and finance, Liverpool, Arsenel","tags":"football"},
                {"id":3,"text":"an article about money and lending","tags":"money"},
                {"id":4,"text":"an article about money and banking and lending","tags":"money"}]


    def test_sklearn_nmf(self):
        corpus = DefaultJsonCorpus(self.get_docs())
        ds = DocumentSimilarity(model_type="sklearn_nmf")
        ds.fit(corpus)
        res = ds.nn(0,k=1)
        self.assertEqual(res[0][0],1)

    def test_gensim_lsi(self):
        corpus = DefaultJsonCorpus(self.get_docs())
        ds = DocumentSimilarity(model_type="gensim_lsi")
        ds.fit(corpus)
        res = ds.nn(0,k=1)
        self.assertEqual(res[0][0],1)


    def test_gensim_rp(self):
        corpus = DefaultJsonCorpus(self.get_docs())
        ds = DocumentSimilarity(model_type="gensim_rp")
        ds.fit(corpus)
        res = ds.nn(0,k=1)
        self.assertEqual(res[0][0],1)

    def test_gensim_lsi(self):
        corpus = DefaultJsonCorpus(self.get_docs())
        ds = DocumentSimilarity(model_type="gensim_lsi")
        ds.fit(corpus)
        score = ds.score(k=1)
        self.assertEqual(score,1.0)




        
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    unittest.main()
