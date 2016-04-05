from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_selection import SelectKBest, chi2
from sklearn.base import BaseEstimator,TransformerMixin
import logging 
import time
import logging

logger = logging.getLogger(__name__)

class Tfidf_transform(BaseEstimator,TransformerMixin):
    """
    Create TF-IDF (term frequency - inverse document frequency) features. 

    can use chi-squared test to limit features. Assumes string based input feature that can be split.
    Uses scikit-learn based transformers internally

    Parameters
    ----------

    min_df : int, optinal
       min document frequency (for sklearn vectorizer)
    max_df : float, optional
       max document frequency (for sklearn vectorizer)
    select_features : bool, optional
       use chi-squared test to select features
    topn_features : int, optional
       keep top features from chi-squared test
    stop_words : str, optional
       stop words (for sklearn vectorizer)
    target_feature : str, optional
       target feature for chi-squared test
    """
    def __init__(self,input_feature=None,output_feature=None,min_df=0,max_df=1.0,select_features=False,topn_features=50000,stop_words=None,target_feature=None,vectorizer=None,tfidf_transformer=None,ch2=None,fnames=None,feature_names_support=[],ngram_range=[1,1]):
        self.input_feature=input_feature
        self.output_feature=output_feature
        self.min_df=min_df
        self.max_df=max_df
        self.select_features = select_features
        self.topn_features=topn_features
        self.stop_words = stop_words
        self.target_feature = target_feature
        self.vectorizer = vectorizer
        self.tfidf_transformer = tfidf_transformer
        self.ch2 = ch2
        self.fnames = fnames
        self.feature_names_support = feature_names_support
        self.ngram_range = ngram_range


    def get_tokens(self,v):
        """basic method to get "document" string from feature
        """
        if isinstance(v, list):
            return " ".join([i if isinstance(i, basestring) else str(i) for i in v])
        elif isinstance(v,basestring):
            return v
        else:
            return str(v)

    
    def fit(self,df):
        """
        Fit tfidf transform

        Parameters
        ----------

        df : pandas dataframe 

        Returns
        -------
        self: object
        """
        self.vectorizer = CountVectorizer(min_df=self.min_df,max_df=self.max_df,stop_words=self.stop_words,ngram_range=self.ngram_range)
        self.tfidf_transformer = TfidfTransformer()
        logger.info("getting docs")
        docs = df[self.input_feature].apply(self.get_tokens)
        logger.info("running vectorizer")
        counts = self.vectorizer.fit_transform(docs.as_matrix())
        logger.info("run tfidf transform")
        self.tfidf = self.tfidf_transformer.fit_transform(counts)
        self.fnames = self.vectorizer.get_feature_names()
        logger.info("base tfidf features %d",len(self.fnames))
        if self.select_features:
            self.ch2 = SelectKBest(chi2, k=self.topn_features)
            self.ch2.fit_transform(self.tfidf, df[self.target_feature])
            self.feature_names_support = set([self.fnames[i] for i in self.ch2.get_support(indices=True)])
            logger.info("selected tfidf features %d",len(self.feature_names_support))
        return self

    def _create_tfidf(self,v):
        s = [self.get_tokens(v)]
        counts = self.vectorizer.transform(s)
        self.tfidf = self.tfidf_transformer.transform(counts)
        doc_tfidf = {}
        for (col,val) in zip(self.tfidf[0].indices,self.tfidf[0].data):
            fname = self.fnames[col]
            if self.select_features:
                if fname in self.feature_names_support:
                    doc_tfidf[fname] = val
            else:
                doc_tfidf[fname] = val
        self.progress += 1
        if self.progress % 100 == 0:
            logger.info("processed %d/%d",self.progress,self.size)
        return doc_tfidf
        

    def transform(self,df):
        """
        transform features with tfidf transform

        Parameters
        ----------

        X : pandas dataframe 

        Returns
        -------
        
        Transformed pandas dataframe
        """
        self.progress = 0
        self.size = df.shape[0]
        df[self.output_feature] = df[self.input_feature].apply(self._create_tfidf)
        return df



