import seldon.pipeline.pipelines as pl
from collections import defaultdict
from  collections import OrderedDict
import logging
import operator
import re
import pandas as pd
import numpy as np
from sklearn.base import BaseEstimator,TransformerMixin

class Binary_transform(BaseEstimator,TransformerMixin):
    """Create a binary feature based on existence of a feature

    """
    def __init__(self,input_feature=None,output_feature=None):
        self.input_feature=input_feature
        self.output_feature=output_feature

    def fit(self,X):
        """nothing to do in fit
        """
        return self

    def transform(self,df):
        df[self.output_feature] = df.apply(lambda row: 1 if not pd.isnull(row[self.input_feature]) and not row[self.input_feature] == 0 and not row[self.input_feature] == "" else 0,axis=1)
        return df

################

class Include_features_transform(BaseEstimator,TransformerMixin):
    """Filter a dataset and include only specided set of features

    Args:
        included (list): list of features to be included
    """
    def __init__(self,included=[]):
        self.included = included

    def fit(self,X):
        """nothing to do in fit
        """
        return self

    def transform(self,df):
        """only include features specified in result
        """
        return df[list(set(self.included).intersection(df.columns))]

################

class Exclude_features_transform(BaseEstimator,TransformerMixin):
    """Filter a dataset and exclude specided set of features

    Args:
        excluded (list): list of features to be excluded
    """
    def __init__(self,excluded=[]):
        self.excluded = excluded

    def fit(self,X):
        """nothing to do in fit
        """
        return self

    def transform(self,df):
        """only include features specified in result
        """
        df = df.drop(self.excluded, axis=1,errors='ignore')
        return df


#############

class Split_transform(BaseEstimator,TransformerMixin):
    """Split a set of string input features on an expression and create a new feature which has a list of values

    Args:
        split_expression (str): regular expression to split feature on

        ignore_numbers (bool): whether to ignore any resulting strings that represent numbers

        input_features (list): list of feature names to split - should all have text values
    """
    def __init__(self,split_expression=" ",ignore_numbers=False,input_features=[],output_feature=None):
        super(Split_transform, self).__init__()
        self.split_expression=split_expression
        self.ignore_numbers=ignore_numbers
        self.input_features=input_features
        self.output_feature = output_feature

    def is_number(self,s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def fit(self,X):
        return self

    def split(self,row):
        ftokens = []
        for col in self.input_features:
            if isinstance(row[col],basestring):
                tokens = re.split(self.split_expression,row[col])
                for token in tokens:
                    token = token.rstrip().lower()
                    if not self.ignore_numbers or (self.ignore_numbers and not self.is_number(token)):
                        ftokens.append(token)
        return pd.Series({'val': ftokens})

    def transform(self,df):
        """transform text features by splitting them and creating a list of feature as result
        """
        df[self.output_feature] = df.apply(self.split,axis=1)
        return df

#############

class Exist_features_transform(BaseEstimator,TransformerMixin):
    """Filter rows based on whether a specified set of features exists

    Args:
        included (list): list of features that need to exist
    """
    def __init__(self,included=None):
        super(Exist_features_transform, self).__init__()
        self.included = included

    def fit(self,objs):
        return self

    def transform(self,df):
        """transform by returning input feature set if required features exist in it
        """
        print "running exists features with ",self.included
        df.dropna(subset=self.included,inplace=True)
        return df

#############

class Svmlight_transform(BaseEstimator,TransformerMixin):
    """take a set of features and transform into a sorted dictionary of numeric id:value features

    Args:
        included (list): set of feature to use as input

        zero_based (bool): whether to start first id at 0

        excluded (list): set of features to exclude
    """
    def __init__(self,included=None,zero_based=False,excluded=[],id_map={},output_feature=None):
        super(Svmlight_transform, self).__init__()
        self.included = included
        self.excluded = excluded
        self.id_map = id_map
        self.zero_based = False
        if self.zero_based:
            self.lastId = 0
        else:
            self.lastId = 1
        self.output_feature=output_feature

    @staticmethod
    def is_number(s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def map(self,v,col):
        if isinstance(v,list):
            return set([col+"_"+lval for lval in v])
        elif isinstance(v,dict):
            return set([col+"_"+k if self.is_number(v) else col+"_"+k+"_"+str(v) for k,v in v.items()])
        else:
            if self.is_number(v):
                return set(col)
            else:
                return set([col+"_"+str(v)])

    def set_id(self,v,col):
        if isinstance(v,list):
            return [(self.id_map[col+"_"+lval],1) for lval in v]
        elif isinstance(v,dict):
            return [(self.id_map[col+"_"+k],v) if self.is_number(v) else (self.id_map[col+"_"+k+"_"+str(v)],1) for k,v in v.items()]
        else:
            if self.is_number(v):
                if not pd.isnull(v):
                    return [(self.id_map[col],v)]
                else:
                    return []
            else:
                return [(self.id_map[col+"_"+v],1)]


    def union(self,vals):
        s = set()
        for v in vals:
            s = s.union(v)
        return s


    def fit(self,df):
        """create ids for each feature to be included
        """
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        features = set()
        df_numeric = df.select_dtypes(include=numerics)
        df_categorical = df.select_dtypes(exclude=numerics)
        for col in df_categorical.columns:
            if (not self.included or col in self.included) and (not col in self.excluded):
                print "SVM transform - Fitting categorical feature ",col
                res = df[col].apply(self.map,col=col)
                s = res.groupby(lambda x : "all").aggregate(self.union)
                features = features.union(s["all"])
        for col in df_numeric.columns:
            if (not self.included or col in self.included) and (not col in self.excluded):
                print "SVM transform - Fitting numerical feature ",col
                features.add(col)
        self.id_map = dict([(v,i+1) for i,v in enumerate(features)])
        return self

    def toDict(self,x):
        return dict(x)

    def transform(self,df):
        """transform features by getting id and numeric value
        """
        df_tmp = pd.DataFrame()
        for col in df.columns:
            if (not self.included or col in self.included) and (not col in self.excluded):
                df_tmp[col] = df[col].apply(self.set_id,col=col)
        df[self.output_feature] = df_tmp.sum(axis=1)
        df[self.output_feature] = df[self.output_feature].apply(self.toDict)
        return df

#############


class Feature_id_transform(BaseEstimator,TransformerMixin):
    """create a numeric feature id

    Args:
        min_size (int): minimum number of examples of each feature value for feature to be included in transform as new id

        exclude_missing (bool): exclude rows that do not have the input feature
    """
    def __init__(self,input_feature=None,output_feature=None,min_size=0,exclude_missing=False,zero_based=False,id_map={}):
        self.input_feature=input_feature
        self.output_feature=output_feature
        self.min_size = min_size
        self.exclude_missing = exclude_missing
        self.id_map = id_map
        self.zero_based = zero_based

    def fit(self,df):
        """create map of ids for each feature value

        create counts of occurrences of each feature value. Exclude features with not enough counds. Create id map.
        """
        if self.input_feature in df:
            counts = df[self.input_feature].value_counts()
            self.id_map = {}
            if self.zero_based:
                idx = 0
            else:
                idx = 1
            for c,v in counts.iteritems():
                if v >= self.min_size:
                    self.id_map[c] = idx
                    idx += 1
                else:
                    break
        return self

    def map(self,v):
        if v in self.id_map:
            return self.id_map[v]
        else:
            return np.nan

    def transform(self,df):
        """transform features creating a new id and exluding rows if needed
        """
        if self.input_feature in df:
            df[self.output_feature] = df[self.input_feature].apply(self.map)
            if self.exclude_missing:
                df = df[pd.notnull(df[self.output_feature])]
                df[self.output_feature] = df[self.output_feature].astype(int)
            else:
                df[self.output_feature] = df[self.output_feature].fillna(-1).astype(int)
        return df




