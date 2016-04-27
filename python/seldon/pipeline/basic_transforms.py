from collections import defaultdict
from  collections import OrderedDict
import logging
import operator
import re
import pandas as pd
import numpy as np
from sklearn.base import BaseEstimator,TransformerMixin
import time

logger = logging.getLogger(__name__)

class Binary_transform(BaseEstimator,TransformerMixin):
    """
    Create a binary feature based on existence of another feature
    
    Parameters
    ----------
    input_feature : str
       input feature to transform
    output_feature : str
       output feature to place transformation
    """
    def __init__(self,input_feature=None,output_feature=None):
        self.input_feature=input_feature
        self.output_feature=output_feature

    def fit(self,X):
        """nothing to do in fit
        """
        return self

    def transform(self,df):
        logger.info("Binary transform")
        """
        Transform a dataframe creating a new binary feature

        Parameters
        ----------

        df : pandas dataframe 

        Returns
        -------
        
        Transformed pandas dataframe

        """
        df[self.output_feature] = df.apply(lambda row: 1 if (not pd.isnull(row[self.input_feature])) and (not row[self.input_feature] == "0") and (not row[self.input_feature] == 0) and (not row[self.input_feature] == "") else 0,axis=1)
        return df

################

class Include_features_transform(BaseEstimator,TransformerMixin):
    """
    Filter a dataset and include only specided set of features


    Parameters
    ----------
    
    input_features : list str
       input features to include
    """
    def __init__(self,included=[]):
        self.included = included

    def fit(self,X):
        """nothing to do in fit
        """
        return self

    def transform(self,df):
        """
        transform a dataframe to include given features

        Parameters
        ----------

        df : pandas dataframe 

        Returns
        -------
        
        Transformed pandas dataframe
        """
        df =  df[list(set(self.included).intersection(df.columns))]
        return df

################

class Exclude_features_transform(BaseEstimator,TransformerMixin):
    """
    Filter a dataset and exclude specided set of features

    Parameters
    ----------

    excluded : list str
       list of features to be excluded
    """
    def __init__(self,excluded=[]):
        self.excluded = excluded

    def fit(self,X):
        """nothing to do in fit
        """
        return self

    def transform(self,df):
        """
        Trasform dataframe to include specified features only

        Parameters
        ----------

        df : pandas dataframe 

        Returns
        -------
        
        Transformed pandas dataframe
        """
        df = df.drop(self.excluded, axis=1,errors='ignore')
        return df


#############

class Split_transform(BaseEstimator,TransformerMixin):
    """
    Split a set of string input features on an expression and create a new feature which has a list of values

    Parameters
    ----------

    split_expression : str
       regular expression to split feature on
    ignore_numbers : bool
       whether to ignore any resulting strings that represent numbers
    input_features : list str
       list of feature names to split - should all have text values
    output_feature : str
       output feature 
    """
    def __init__(self,split_expression=" ",ignore_numbers=False,input_features=[],output_feature=None):
        super(Split_transform, self).__init__()
        self.split_expression=split_expression
        self.ignore_numbers=ignore_numbers
        self.input_features=input_features
        self.output_feature = output_feature

    def _is_number(self,s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def fit(self,X):
        return self

    def _split(self,row):
        ftokens = []
        for col in self.input_features:
            if isinstance(row[col],basestring):
                tokens = re.split(self.split_expression,row[col])
                for token in tokens:
                    token = token.rstrip().lower()
                    if not self.ignore_numbers or (self.ignore_numbers and not self._is_number(token)):
                        ftokens.append(token)
        return pd.Series({'val': ftokens})

    def transform(self,df):
        """
        Transform text features by splitting them and creating a list of feature as result

        Parameters
        ----------

        df : pandas dataframe 

        Returns
        -------
        
        Transformed pandas dataframe
        """
        df[self.output_feature] = df.apply(self._split,axis=1)
        return df

#############

class Exist_features_transform(BaseEstimator,TransformerMixin):
    """Filter rows based on whether a specified set of features exists

    Parameters
    ----------
    included : list str
       list of features that need to exist
    """
    def __init__(self,included=None):
        super(Exist_features_transform, self).__init__()
        self.included = included

    def fit(self,objs):
        return self

    def transform(self,df):
        """
        Transform by returning input feature set if required features exist in it

        Parameters
        ----------

        df : pandas dataframe 

        Returns
        -------
        
        Transformed pandas dataframe
        """
        df.dropna(subset=self.included,inplace=True)
        return df

#############

class Svmlight_transform(BaseEstimator,TransformerMixin):
    """
    Take a set of features and transform into a sorted dictionary of numeric id:value features

    Parameters
    ----------

    included : list str
       set of feature to use as input
    zero_based : zero_based, optional
       whether to start first id at 0
    excluded : list str
       set of features to exclude
    """
    def __init__(self,included=None,zero_based=False,excluded=[],id_map={},output_feature=None,id_map_file=None):
        super(Svmlight_transform, self).__init__()
        self.included = included
        self.excluded = excluded
        self.id_map = id_map
        self.zero_based = zero_based
        self.output_feature=output_feature
        self.id_map_file = id_map_file

    @staticmethod
    def _is_number(s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def _map(self,v,col):
        if isinstance(v,list):
            return set([col+"_"+lval for lval in v])
        elif isinstance(v,dict):
            return set([col+"_"+k if self._is_number(v) else col+"_"+k+"_"+str(v) for k,v in v.items()])
        else:
            if self._is_number(v):
                return set(col)
            elif isinstance(v, basestring):
                return set([col+"_"+v])
            else:
                return set([col+"_"+str(v)])

    def _set_id(self,row):
        lvals = []
        for col in row.index.values:
            if (not self.included or col in self.included) and (not col in self.excluded):
                v = row[col]
                if isinstance(v,list):
                    lvals += [(self.id_map[col+"_"+lval],1) for lval in v]
                elif isinstance(v,dict):
                    lvals += [(self.id_map[col+"_"+k],v) if self._is_number(v) else (self.id_map[col+"_"+k+"_"+str(v)],1) for k,v in v.items()]
                else:
                    if self._is_number(v):
                        if not pd.isnull(v):
                            lvals += [(self.id_map[col],v)]
                    else:
                        var_name = col+"_"+v
                        if var_name in self.id_map:
                            lvals += [(self.id_map[var_name],1)]
        self.progress += 1
        if self.progress % 100 == 0:
            logger.info("processed %d/%d",self.progress,self.size)
        return pd.Series([sorted(lvals)])


    def _union(self,vals):
        s = set()
        for v in vals:
            s = s.union(v)
        return s

    def _save_id_map(self):
        import unicodecsv
        writer = unicodecsv.writer(open(self.id_map_file, 'wb'))
        for key, value in self.id_map.items():
            writer.writerow([value, key])



    def fit(self,df):
        """
        create ids for each feature to be included

        Parameters
        ----------

        df : pandas dataframe 

        Returns
        -------
        self: object
        """
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        features = set()
        df_numeric = df.select_dtypes(include=numerics)
        df_categorical = df.select_dtypes(exclude=numerics)
        for col in df_categorical.columns:
            if (not self.included or col in self.included) and (not col in self.excluded):
                logger.info("SVM transform - Fitting categorical feature %s" % col)
                res = df[col].apply(self._map,col=col)
                s = res.groupby(lambda x : "all").aggregate(self._union)
                features = features.union(s["all"])
        for col in df_numeric.columns:
            if (not self.included or col in self.included) and (not col in self.excluded):
                logger.info("SVM transform - Fitting numerical feature %s" % col)
                features.add(col)
        inc = 1
        if self.zero_based:
            inc = 0
        self.id_map = dict([(v,i+inc) for i,v in enumerate(features)])
        if not self.id_map_file is None:
            self._save_id_map()
        return self

    def transform(self,df):
        """
        Transform features by getting id and numeric value

        Parameters
        ----------

        X : pandas dataframe 

        Returns
        -------
        
        Transformed pandas dataframe
        """
        self.progress = 0
        self.size = df.shape[0]
        df[self.output_feature] = df.apply(self._set_id,axis=1,reduce=True)
        return df

#############


class Feature_id_transform(BaseEstimator,TransformerMixin):
    """create a numeric feature id

    Parameters
    ----------
    
    input_feature : str
       input feature to create ids from
    output_feature : str
       output feature to place ids
    min_size : int, optional
       minimum number of examples of each feature value for feature to be included in transform as new id
    exclude_missing : bool, optional
       exclude rows that do not have the input feature
    """
    def __init__(self,input_feature=None,output_feature=None,min_size=0,max_classes=1000,exclude_missing=False,zero_based=False,id_map={}):
        self.input_feature=input_feature
        self.output_feature=output_feature
        self.min_size = min_size
        self.exclude_missing = exclude_missing
        self.id_map = id_map
        self.zero_based = zero_based
        self.max_classes=max_classes

    def fit(self,df):
        """
        Create map of ids for each feature value

        create counts of occurrences of each feature value. Exclude features with not enough counds. Create id map.

        Parameters
        ----------

        df : pandas dataframe 

        Returns
        -------
        self: object

        """
        if self.input_feature in df:
            counts = df[self.input_feature].value_counts()
            sorted_counts = sorted(counts.iteritems(), key=operator.itemgetter(1),reverse=True)
            self.id_map = {}
            if self.zero_based:
                idx = 0
            else:
                idx = 1
            for c,v in sorted_counts:
                if v >= self.min_size and len(self.id_map)<self.max_classes:
                    self.id_map[c] = idx
                    idx += 1
                else:
                    break
        return self

    def _map(self,v):
        if v in self.id_map:
            return self.id_map[v]
        else:
            return np.nan

    def transform(self,df):
        """
        Transform features creating a new id and exluding rows if needed

        Parameters
        ----------

        df : pandas dataframe 

        Returns
        -------
        
        Transformed pandas dataframe

        """
        if self.input_feature in df:
            df[self.output_feature] = df[self.input_feature].apply(self._map)
            if self.exclude_missing:
                df = df[pd.notnull(df[self.output_feature])]
                df[self.output_feature] = df[self.output_feature].astype(int)
            else:
                df[self.output_feature] = df[self.output_feature].fillna(-1).astype(int)
        return df




