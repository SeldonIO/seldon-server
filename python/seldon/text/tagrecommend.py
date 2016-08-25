from collections import defaultdict
from seldon.pipeline.pandas_pipelines import BasePandasEstimator 
from sklearn.base import BaseEstimator,ClassifierMixin
import logging
import operator
from seldon.util import DeprecationHelper

logger = logging.getLogger(__name__)


class TagRecommender(BaseEstimator):

    def __init__(self,max_s2_size=0.1,min_s2_size=25,min_score=0.0):
        """
        Simple tag recommender using jaccard or asymetric cooccurrence of tags as discussed in Borkur Sigurbjornsson and Roelof van Zwol. 2008. Flickr tag recommendation based on collective knowledge. In Proceedings of the 17th international conference on World Wide Web (WWW 08). ACM, New York, NY, USA, 327-336

        max_s2_size : int, optional
           max percentage size of candidate tag documents for jaccard distance calc
        min_s2_size : int, optional
           min absolute size of candidate tag documents for asymmetric coccurrence score
        min_score : float
           min score for any tag for it to be returned
        """
        self.tag_map = defaultdict(set)
        self.max_s2_size=max_s2_size
        self.min_s2_size=min_s2_size
        self.min_score=min_score
        
    def fit(self,corpus,split_char=','):
        """
        Process a corpus and fir data.

        Parameters
        ----------

        corpus : object
           a corpus object that follows seldon.text.DefaultJsonCorpus
        split_char : str
           character to split tags 

        Returns
        -------
        
        trained TagRecommender object

        """
        processed = 0
        for j in corpus.get_meta():
            processed += 1
            if processed % 1000 == 0:
                logger.info("Processed %s",processed)
            doc_id = j['id']
            for tag in j['tags'].split(split_char):
                self.tag_map[tag].add(long(doc_id))
        self.tag_map_size = float(len(self.tag_map))
        return self

    def jaccard(self,s1,s2,max_s2_size=0.1):
        """
        Return jaccard distance between two sets (of documents)

        Parameters
        ----------

        s1 : set
           set (of document ids)
        s2 : set 
           set (of document ids)
        max_s2_size : int, optional
           the max percentage size of s2 for a result to be returned. Can be set to ignore very popular tags returning non-zero scores 
        """
        p = len(s1)/self.tag_map_size
        if p <= max_s2_size:
            intersection_size = len(s1.union(s2))
            if intersection_size > 0:
                return len(s1.intersection(s2))/float(intersection_size)
            else:
                return 0.0
        else:
            return 0.0

    def asymmetric_occur(self,s1,s2,min_s2_size=25):
        """
        Return asymmetric occurrence of set s1 against s2

        Parameters
        ----------

        s1 : set
           set (of document ids)
        s2 : set 
           set (of document ids)
        min_s2_size : int, optional
           the absolute min number of documents in s2. Increase to stop very unlikely tags being recommended.
        """        
        s2_size = len(s2)
        if s2_size >= min_s2_size:
            return len(s1.intersection(s2))/float(len(s2))
        else:
            return 0.0

    def knn(self,tag,k=5,metric='jaccard',exclusions=[]):
        """
        Get k nearest neighbours of a tag

        Parameters
        ----------

        tag : str
           query tag
        k : int
           number of neighbours to return
        metric : str
           metric to use, 'jaccard' or 'asym'
        excclusions : list of str
           tags to exclude

        Returns
        -------

        list of tuples of tag,score
        """
        scores = {}
        tag_sig = self.tag_map[tag]
        for tag_candidate in self.tag_map:
            if not (tag == tag_candidate or tag_candidate in exclusions):
                score = 0.0
                if metric == 'jaccard' or metric== 'both':
                    score += self.jaccard(tag_sig,self.tag_map[tag_candidate],max_s2_size=self.max_s2_size)
                if metric == 'asym' or metric== 'both':
                    score += self.asymmetric_occur(tag_sig,self.tag_map[tag_candidate],min_s2_size=self.min_s2_size)
                if score > 0:
                    scores[tag_candidate] = score
        sorted_scores = sorted(scores.items(), key=operator.itemgetter(1),reverse=True)
        top_scores = sorted_scores[:k]
        f_scores = []
        for (tag,score) in top_scores:
            if score < self.min_score:
                break
            else:
                f_scores.append((tag,score))
        return f_scores

    def recommend(self,tags,k=5,knn_k=5,metric='both'):
        """
        recommend tags for a given set of tags

        Parameters
        ----------

        tags : str
           query tags
        k : int
           number of tags to return
        knn_k : int
           number of nearest neighbours for each tag to collect
        metric : str
           metric to use, 'jaccard' or 'asym'

        Returns
        -------

        sorted list of tuples of tag,score

        """
        scores = defaultdict(float)
        for tag in tags:
            for (tag_recommended,score) in self.knn(tag,k=knn_k,metric=metric,exclusions=tags):
                scores[tag_recommended] += score
        sorted_scores = sorted(scores.items(), key=operator.itemgetter(1),reverse=True)
        return sorted_scores[:k]

Tag_Recommender = DeprecationHelper(TagRecommender)