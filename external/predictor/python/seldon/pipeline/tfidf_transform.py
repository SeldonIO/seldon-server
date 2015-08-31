import seldon.pipeline.pipelines as pl
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_selection import SelectKBest, chi2
import logging 

class Tfidf_transform(pl.Feature_transform):

    def __init__(self,min_df=10,max_df=0.7,select_features=False,topn_features=50000,stop_words=None,target_feature=None):
        super(Tfidf_transform, self).__init__()
        self.min_df=min_df
        self.max_df=max_df
        self.select_features = select_features
        self.topn_features=topn_features
        self.stop_words = stop_words
        self.target_feature = target_feature
        self.ch2 = ""

    def getTokens(self,j):
        if self.input_feature in j:
            if isinstance(j[self.input_feature], list):
                return " ".join(map(str,j[self.input_feature]))
            else:
                return str(j[self.input_feature])
        else:
            return ""

    def get_models(self):
        return super(Tfidf_transform, self).get_models() + [(self.min_df,self.max_df,self.select_features,self.topn_features,self.stop_words,self.target_feature),self.vectorizer,self.tfidf_transformer,self.ch2,self.fnames,self.feature_names_support]
    
    def set_models(self,models):
        models = super(Tfidf_transform, self).set_models(models)
        (self.min_df,self.max_df,self.select_features,self.topn_features,self.stop_words,self.target_feature) = models[0]
        self.vectorizer = models[1]
        self.tfidf_transformer = models[2]
        self.ch2 = models[3]
        self.fnames = models[4]
        self.feature_names_support = models[5]

    def fit(self,objs):
        docs = []
        target = []
        self.vectorizer = CountVectorizer(min_df=self.min_df,max_df=self.max_df,stop_words=self.stop_words)
        self.tfidf_transformer = TfidfTransformer()
        for j in objs:
            docs.append(self.getTokens(j))
            if self.target_feature:
                target.append(int(j[self.target_feature]))
        counts = self.vectorizer.fit_transform(docs)
        self.tfidf = self.tfidf_transformer.fit_transform(counts)
        self.fnames = self.vectorizer.get_feature_names()
        self.logger.info("%s base tfidf features %d",self.get_log_prefix(),len(self.fnames))
        if self.select_features:
            self.ch2 = SelectKBest(chi2, k=self.topn_features)
            self.ch2.fit_transform(self.tfidf, target)
            self.feature_names_support = set([self.fnames[i] for i in self.ch2.get_support(indices=True)])
            self.logger.info("%s selected tfidf features %d",self.get_log_prefix(),len(self.feature_names_support))

    def transform(self,j):
        docs = []
        docs.append(self.getTokens(j))
        counts = self.vectorizer.transform(docs)
        self.tfidf = self.tfidf_transformer.transform(counts)
        if self.select_features:
            self.ch2.transform(self.tfidf)
        doc_tfidf = {}
        for (col,val) in zip(self.tfidf[0].indices,self.tfidf[0].data):
            fname = self.fnames[col]
            if self.select_features:
                if fname in self.feature_names_support:
                    doc_tfidf[fname] = val
            else:
                doc_tfidf[fname] = val
        j[self.output_feature] = doc_tfidf
        return j


