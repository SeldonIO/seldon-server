from collections import defaultdict
from seldon.pipeline.pandas_pipelines import BasePandasEstimator 
from sklearn.base import BaseEstimator,ClassifierMixin
import logging
import operator
#from profilehooks import profile

logger = logging.getLogger('seldon.text.tagrecommed')


class Tag_Recommender(BaseEstimator):

    def __init__(self,max_s2_size=0.1,min_s2_size=25):
        self.tag_map = defaultdict(set)
        self.max_s2_size=max_s2_size
        self.min_s2_size=min_s2_size

    def fit(self,corpus,split_char=','):
        processed = 0
        for j in corpus.get_meta():
            processed += 1
            if processed % 1000 == 0:
                logger.info("Processed %s",processed)
            doc_id = j['id']
            for tag in j['tags'].split(split_char):
                self.tag_map[tag].add(long(doc_id))
        self.tag_map_size = float(len(self.tag_map))

    def jaccard(self,s1,s2,max_s2_size=0.1):
        p = len(s1)/self.tag_map_size
        if p <= max_s2_size:
            intersection_size = len(s1.union(s2))
            if intersection_size > 0:
                return len(s1.intersection(s2))/float(intersection_size)
            else:
                return 0.0
        else:
            return 0.0

    def conditional_proba(self,s1,s2,min_s2_size=25):
        s2_size = len(s2)
        if s2_size >= min_s2_size:
            return len(s1.intersection(s2))/float(len(s2))
        else:
            return 0.0

    def knn(self,tag,k=5,metric='jaccard',exclusions=[]):
        scores = {}
        tag_sig = self.tag_map[tag]
        for tag_candidate in self.tag_map:
            if not (tag == tag_candidate or tag_candidate in exclusions):
                score = 0.0
                if metric == 'jaccard' or metric== 'both':
                    score += self.jaccard(tag_sig,self.tag_map[tag_candidate],max_s2_size=self.max_s2_size)
                if metric == 'conditional' or metric== 'both':
                    score += self.conditional_proba(tag_sig,self.tag_map[tag_candidate],min_s2_size=self.min_s2_size)
                if score > 0:
                    scores[tag_candidate] = score
        sorted_scores = sorted(scores.items(), key=operator.itemgetter(1),reverse=True)
        return sorted_scores[:k]

    #@profile
    def recommend(self,tags,k=5,knn_k=5,metric='both'):
        scores = defaultdict(float)
        for tag in tags:
            for (tag_recommended,score) in self.knn(tag,k=knn_k,metric=metric,exclusions=tags):
                scores[tag_recommended] += score
        sorted_scores = sorted(scores.items(), key=operator.itemgetter(1),reverse=True)
        return sorted_scores[:k]
