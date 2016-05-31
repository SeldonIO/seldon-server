import seldon.fileutil as fu
import json
from sklearn.externals import joblib
from sklearn.feature_extraction import DictVectorizer
import os.path
import logging
import shutil 
import unicodecsv
import numpy as np
import pandas as pd
import random
import string

class BasePandasEstimator(object):
    """
    Tools to help with Pandas based estimators.

    Parameters
    ----------
    
    target : str
       Target column
    target_readable : str
       More descriptive version of target variable
    included : list str, optional
       columns to include
    excluded : list str, optional
       columns to exclude
    id_map : dict (int,str), optional
       map of class ids to high level names
    """
    def __init__(self, target=None, target_readable=None,included=None,excluded=None,id_map={}):
        self.target = target
        self.target_readable = target_readable
        self.id_map=id_map
        self.included = included
        self.excluded = excluded
        if self.excluded is None:
            self.excluded = []
        if not self.target_readable is None:
            self.excluded.append(self.target_readable)

    def get_target(self):
        return self.target

    def set_class_id_map(self,id_map):
        self.id_map = id_map

    def get_class_id_map(self):
        return self.id_map

    def create_class_id_map(self,df,target,target_readable,zero_based=True):
        """
        Create a map of classification ids to readable values 
        """
        ids = df.drop_duplicates([target,target_readable]).to_dict(orient='records')
        m = {}
        for d in ids:
            if zero_based:
                m[d[target]] = d[target_readable]
            else:
                m[d[target]-1] = d[target_readable]
        self.set_class_id_map(m)

    def encode_onehot(self,df, cols, vec, op):
        """
        One hot encode categorical values from a data frame using a vectorizer passed in
        """
        if op == "fit":
            vec_data = pd.DataFrame(vec.fit_transform(df[cols].to_dict(outtype='records')).toarray())
        else:
            vec_data = pd.DataFrame(vec.transform(df[cols].to_dict(outtype='records')).toarray())
        vec_data.columns = vec.get_feature_names()
        vec_data.index = df.index
        
        df = df.drop(cols, axis=1)
        df = df.join(vec_data)
        return df

    def convert_dataframe(self,df_base,vectorizer):
        """
        Convert a dataframe into one for use with ml algorithms
        One hot encode the categorical variable
        Ignore date values
        concatenate with numeric values
        """
        if vectorizer is None:
            vectorizer = DictVectorizer()
            op = "fit"
        else:
            op = "transform"
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        # will ignore all date columns
        df_numeric = df_base.select_dtypes(include=numerics)
        df_categorical = df_base.select_dtypes(exclude=numerics+['datetime64[ns]'])
        cat_cols = []
        if len(df_categorical.columns) > 0:
            df_categorical = self.encode_onehot(df_categorical, cols=df_categorical.columns,vec=vectorizer,op=op)
            df_X = pd.concat([df_numeric, df_categorical], axis=1)
        else:
            df_X = df_numeric
        return (df_X,vectorizer)

    def _exclude_include_features(self,df):
        """
        Utility function to include and exclude features from a data frame to create a new one
        """
        if not self.included is None:
            df = df[list(set(self.included+[self.target]).intersection(df.columns))]
        if not self.excluded is None:
            df = df.drop(set(self.excluded).intersection(df.columns), axis=1)
        return df

    def convert_numpy(self,df):
        """
        Convert a dataframe into a numpy data matrix for training and an array of target values
        Uses a vectorizer for one hot encoding which is returned
        NaNs are filled with zeros

        Parameters
        ----------

        df : pandas dataframe

        Returns
        -------

        X : array like - data as array
        y : array - target labels
        vectorizer : vectorizer used for one hot encoding
        """
        if self.target in df:
            if not self.target_readable is None:
                self.create_class_id_map(df,self.target,self.target_readable)
            df_y = df[self.target]
            df_base = df.drop([self.target], axis=1)
        else:
            df_y = None
            df_base = df
        df_base = self._exclude_include_features(df_base)
        df_base = df_base.fillna(0)

        (df_X,self.vectorizer) = self.convert_dataframe(df_base,self.vectorizer)
        return (df_X.as_matrix(),df_y,self.vectorizer)


    def close(self):
        pass


    def predict(self,X):
        proba = self.predict_proba(X)
        return np.argmax(proba, axis=1)
