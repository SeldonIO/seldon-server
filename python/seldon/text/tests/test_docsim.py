import unittest
from seldon.text import DocumentSimilarity
import logging
from gensim import interfaces, utils
from gensim.corpora.dictionary import Dictionary

class TestCorpus(object):

    def __init__(self):
        self.docs = [{"id":"d1","text":"an article about sports and football, Arsenel, Liverpool","tags":"football"},
                {"id":"d2","text":"an article about football and finance, Liverpool, Arsenel","tags":"football"},
                {"id":"d3","text":"an article about money and lending","tags":"money"},
                {"id":"d3","text":"an article about money and banking and lending","tags":"money"}]
        self.dictionary = Dictionary()
        self.dictionary.add_documents(self.get_texts())

    def get_texts(self,raw=False):
        for j in self.docs:
            text = j["text"]
            if raw:
                yield text
            else:
                yield utils.tokenize(text, deacc=True, lowercase=True)

    def __iter__(self):
        for text in self.get_texts():
            yield self.dictionary.doc2bow(text, allow_update=False)

    def get_meta(self):
        doc_id = 0
        for j in self.docs:
            m = {}
            m["id"] = j["id"]
            m['tags'] = j['tags']
            m['corpus_seq_id'] = doc_id
            doc_id += 1
            yield m

    def get_dictionary(self):
        return self.dictionary


class Test_docsim(unittest.TestCase):

    def test_sklearn_nmf(self):
        corpus = TestCorpus()
        ds = DocumentSimilarity(model_type="sklearn_nmf")
        ds.fit(corpus)
        res = ds.nn(0,k=1)
        self.assertEqual(res[0][0],1)

    def test_gensim_lsi(self):
        corpus = TestCorpus()
        ds = DocumentSimilarity(model_type="gensim_lsi")
        ds.fit(corpus)
        res = ds.nn(0,k=1)
        self.assertEqual(res[0][0],1)


    def test_gensim_rp(self):
        corpus = TestCorpus()
        ds = DocumentSimilarity(model_type="gensim_rp")
        ds.fit(corpus)
        res = ds.nn(0,k=1)
        self.assertEqual(res[0][0],1)

    def test_gensim_lsi(self):
        corpus = TestCorpus()
        ds = DocumentSimilarity(model_type="gensim_lsi")
        ds.fit(corpus)
        score = ds.score(k=1)
        self.assertEqual(score,1.0)




        
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    unittest.main()
