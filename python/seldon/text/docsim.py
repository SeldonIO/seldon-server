from gensim.models.doc2vec import LabeledSentence
import gensim
from seldon import Recommender
from gensim.corpora.textcorpus import TextCorpus
from gensim import interfaces, utils
from six import string_types
from gensim.corpora.dictionary import Dictionary
from gensim import corpora, models, similarities
import annoy
import json
import codecs 
from nltk.corpus import stopwords
import logging
import scipy
import time
import glob
import shutil
import operator
from collections import defaultdict
import copy

logger = logging.getLogger(__name__)

def jaccard(s1,s2):
    return len(s1.intersection(s2))/float(len(s1.union(s2)))

current_milli_time = lambda: int(round(time.time() * 1000))

class DefaultJsonCorpus(object):
    """
    A default JSON corpus based on gensim TextCorpus. It assumes a file or list of JSON as input.
    The methods provided by gensim TextCorpus are needed for the GenSim training.
    Any corpus provided to DocumentSimilarity should provide the methods given in this class.
    """
    def __init__(self, input=None,create_dictionary=True):
        super(DefaultJsonCorpus, self).__init__()
        self.input = input
        self.dictionary = Dictionary()
        self.metadata = False
        if create_dictionary:
            self.dictionary.add_documents(self.get_texts())


    def __iter__(self):
        for text in self.get_texts():
            yield self.dictionary.doc2bow(text, allow_update=False)

    def getstream(self):
        return utils.file_or_filename(self.input)

    def __len__(self):
        if not hasattr(self, 'length'):
            # cache the corpus length
            self.length = sum(1 for _ in self.get_texts())
        return self.length

    def get_json(self):
        if isinstance(self.input,list):
            for j in self.input:
                yield j
        else:
            with self.getstream() as lines:
                for line in lines:
                    line = line.rstrip()
                    j = json.loads(line)
                    yield j

    def get_texts(self,raw=False):
        """
        yield raw text or tokenized text
        """
        for j in self.get_json():
            text = j["text"]
            if raw:
                yield text
            else:
                yield utils.tokenize(text, deacc=True, lowercase=True)

    def get_meta(self):
        """
        return a json object with meta data for the documents. It must return:
        id - id for this document
        optional title and tags. Tags will be used as base truth used to score document similarity results.
        """
        doc_id = 0
        for j in self.get_json():
            m = copy.deepcopy(j)
            m['id'] = long(m['id'])
            m['corpus_seq_id'] = doc_id
            doc_id += 1
            yield m

    def get_dictionary(self):
        return self.dictionary



class DocumentSimilarity(Recommender):

    """

    Parameters
    ----------

    model_type : string
       gensim_lsi,gensim_lda,gensim_rp,sklearn_nmf
    vec_size : int
       vector size of model
    annoy_trees : int
       number of trees to create for Annoy approx nearest neighbour
    work_folder : str
       folder for tmp files
    sklearn_tfidf_args : dict, optional
       args to pass to sklearn TfidfVectorizer
    sklear_nmf_args : dict, optional
       args to pass to sklearn NMF model
    """
    def __init__(self,model_type='gensim_lsi',vec_size=100,annoy_trees=100,work_folder="/tmp",sklearn_tfidf_args={'stop_words':"english"},sklearn_nmf_args={"random_state":1,"alpha":.1,"l1_ratio":.5}):
        if not (model_type == 'gensim_lsi' or model_type == 'gensim_lda' or model_type == 'gensim_rp' or model_type == 'sklearn_nmf'):
            raise ValueError("Unknown model type")
        self.model_type=model_type
        self.vec_size=vec_size
        self.annoy_trees=annoy_trees
        self.gensim_output_prefix = "gensim_index"
        self.annoy_output_prefix = "annoy_index"
        self.meta_output_prefix = "meta"
        self.sklearn_tfidf_args = sklearn_tfidf_args
        self.sklearn_nmf_args = sklearn_nmf_args
        self.work_folder=work_folder

    def __getstate__(self):
        """
        Remove things that should not be pickled as they are handled in save/load
        """
        result = self.__dict__.copy()
        del result['index']
        del result['index_annoy']
        del result['seq2meta']
        del result['id2meta']
        return result

    def __setstate__(self, dict):
        self.__dict__ = dict
        


    def create_gensim_model(self,corpus):
        """
        Create a gensim model

        Parameters
        ----------

        corpus : an object that satisfies a gensim TextCorpus

        Returns
        -------
        
        gensim corpus model
        """
        dictionary = corpus.get_dictionary()
        tfidf = models.TfidfModel(corpus)
        corpus_tfidf = tfidf[corpus]
        if self.model_type=='gensim_lsi':
            logger.info("Building gensim lsi model")
            model = models.LsiModel(corpus_tfidf, id2word=dictionary, num_topics=self.vec_size)
        elif self.model_type=='gensim_lda':
            logger.info("Building gensim lda model")
            model = models.LdaModel(corpus_tfidf, id2word=dictionary, num_topics=self.vec_size,passes=10)
        else:
            logger.info("Building gensim random projection model")
            model = models.RpModel(corpus_tfidf, id2word=dictionary, num_topics=self.vec_size)
        return model[corpus_tfidf]

    def create_sklearn_model(self,corpus):
        """
        Create a sklearn model

        Parameters
        ----------

        corpus : object 
           a corpus object that has get_text(raw=True) method

        Returns
        -------
        
        gensim corpus model
        """
        from sklearn.feature_extraction.text import TfidfVectorizer,CountVectorizer
        from sklearn.decomposition import NMF
        #make tfidf and NMF args configurable
        tfidf_vectorizer = TfidfVectorizer(**self.sklearn_tfidf_args)
        tfidf = tfidf_vectorizer.fit_transform(corpus.get_texts(raw=True))
        logger.info("Building sklearn NMF model")
        W = NMF(n_components=self.vec_size, **self.sklearn_nmf_args).fit_transform(tfidf)
        return gensim.matutils.Dense2Corpus(W.T)


    def fit(self,corpus):
        """
        Fit a document similarity model

        Parameters
        ----------

        corpus : object
           a corpus object that follows DefaultJsonCorpus

        Returns
        -------
        
        trained DocumentSimilarity object
        """
        if self.model_type == 'sklearn_nmf':
            model = self.create_sklearn_model(corpus)
        else:
            model = self.create_gensim_model(corpus)

        self.index = similarities.Similarity(self.work_folder+"/gensim_index",model,self.vec_size)
        self.index_annoy = annoy.AnnoyIndex(self.vec_size, metric='angular')
        for i, vec in enumerate(model):
            self.index_annoy.add_item(i, list(gensim.matutils.sparse2full(vec, self.vec_size).astype(float)))
        self.index_annoy.build(self.annoy_trees)
        self.seq2meta = {}
        self.id2meta = {}
        for j in corpus.get_meta():
            self.seq2meta[j['corpus_seq_id']] = j
            self.id2meta[j['id']] = j
        return self

    def save(self,folder):
        """
        save models to folder

        Parameters
        ----------

        folder : str
           saved location folder
        """
        self.index.close_shard()
        for f in glob.glob(self.work_folder+"/gensim_index*"):
            shutil.move(f, folder)
        self.index.output_prefix=folder+"/gensim_index"
        self.index.check_moved()
        self.index.save(folder+"/"+self.gensim_output_prefix)
        self.index_annoy.save(folder+"/"+self.annoy_output_prefix)
        fout = codecs.open(folder+"/"+self.meta_output_prefix, "w", "utf-8")
        for k in self.seq2meta:
            jStr = json.dumps(self.seq2meta[k],sort_keys=True)
            fout.write(jStr+"\n")
        fout.close()

    def load(self,folder):
        """
        load models from folder

        Parameters
        ----------

        folder : str
           location of models
        """
        self.index =  similarities.Similarity.load(folder+"/"+self.gensim_output_prefix)
        self.index.output_prefix=folder+"/gensim_index"
        self.index.check_moved()
        self.index_annoy = annoy.AnnoyIndex(self.vec_size)
        self.index_annoy.load(folder+"/"+self.annoy_output_prefix)
        self.seq2meta = {}
        self.id2meta = {}
        with open(folder+"/"+self.meta_output_prefix) as f:
            for line in f:
                line = line.rstrip()
                j = json.loads(line)
                self.seq2meta[j['corpus_seq_id']] = j
                self.id2meta[j['id']] = j

    def _remove_query_doc(self,query_id,results):
        transformed = []
        for (doc_id,score) in results:
            if not doc_id == query_id:
                transformed.append((doc_id,score))
        return transformed

    def recommend(self,user=None,ids=[],recent_interactions=[],client=None,limit=1):
        if ids is None or len(ids) == 0:
            scores = defaultdict(float)
            for doc_id in recent_interactions:
                doc_scores = self.nn(doc_id,k=limit*10,translate_id=True,approx=True)
                for (doc_id,score) in doc_scores:
                    scores[doc_id] += score
            sorted_x = sorted(scores.items(), key=operator.itemgetter(1))
            sorted_x = sorted_x[::-1]
            return sorted_x[:limit]
        else:
            return []

    def nn(self,doc_id,k=1,translate_id=False,approx=False):
        """
        nearest neighbour query

        Parameters
        ----------

        doc_id : long
           internal or external document id
        k : int
           number of neighbours to return
        translate_id : bool
           translate doc_id into internal id
        approx : bool
           run approx nearest neighbour search using Annoy

        Returns
        -------

        list of pairs of document id (internal or external) and similarity metric in range (0,1)
        """        
        if translate_id:
            doc_id_internal = self.id2meta[doc_id]['corpus_seq_id']
        else:
            doc_id_internal = doc_id
        k += 1
        self.index.num_best = k
        if approx:
            v = self.index.vector_by_id(doc_id_internal)
            if scipy.sparse.issparse(v):
                v_list = []
                for j,v in zip(v.indices, v.data):
                    v_list.append((j,v))
                v = gensim.matutils.sparse2full(v_list, self.vec_size)
            (ids,scores) = self.index_annoy.get_nns_by_vector(v, k, search_k=-1, include_distances=True)
            scores = [1.0-score for score in scores]
            sims = zip(ids,scores)
        else:
            sims =  list(self.index.similarity_by_id(doc_id_internal))
        if translate_id:
            res = []
            for (sim_id,score) in sims:
                if not sim_id == doc_id_internal:
                    res.append((self.seq2meta[sim_id]['id'],score))
            return res
        else:
            return self._remove_query_doc(doc_id_internal,sims)
            
    def get_meta(self,doc_id):
        return self.id2meta[doc_id]


    def score(self,k=1,approx=False):
        """
        score a model

        Parameters
        ----------

        k : int
           number of neighbours to return
        approx : bool
           run approx nearest neighbour search using Annoy

        Returns
        -------

        accuracy metric - avg jaccard distance of returned tags to ground truth tags in meta data
        """        
        start = current_milli_time()
        score_sum = 0
        num_docs = len(self.seq2meta)
        for query_id in range(0,num_docs):
            meta_correct = self.seq2meta[query_id]
            meta_correct_tags = set(meta_correct['tags'].split(','))
            sims =  self.nn(query_id,k,approx=approx)
            doc_score_sum = 0
            doc_scored = 0
            for (doc_id,sim) in sims:
                meta_doc = self.seq2meta[doc_id]
                meta_doc_tags = set(meta_doc['tags'].split(','))
                logger.debug("%s %s",meta_correct_tags,meta_doc_tags)
                score = jaccard(meta_correct_tags,meta_doc_tags)
                logger.debug("score %s %s",meta_doc['id'],score)
                doc_scored += 1
                doc_score_sum += score
            if doc_scored > 0:
                score_sum += doc_score_sum/float(doc_scored)
        end = current_milli_time()
        duration_secs = (end-start)/1000
        duration_per_call = duration_secs/float(num_docs)
        accuracy = score_sum/float(num_docs)
        logger.info("accuracy: %f time: %d secs avg_call_time: %f",accuracy,duration_secs,duration_per_call)
        return accuracy


