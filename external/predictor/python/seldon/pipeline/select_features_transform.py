import seldon.pipeline.pipelines as pl
import numpy as np
import pandas as pd
import math
from sklearn.feature_selection import VarianceThreshold

class Select_features_transform(pl.Feature_transform):

    def __init__(self,exclude=[],use_variance=True,min_variance=0.0,min_cat_percent=0.0,max_cat_percent=1.0):
        self.exclude = exclude
        self.use_variance = use_variance
        self.min_variance = 0.0
        self.features_toremove = []
        self.min_cat_percent=min_cat_percent
        self.max_cat_percent=max_cat_percent
        self.cat_values_toremove = {}

    def get_models(self):
        return [(self.exclude,self.use_variance,self.min_variance,self.features_toremove,self.min_cat_percent,self.max_cat_percent,self.cat_values_toremove)]
    
    def set_models(self,models):
        (self.exclude,self.use_variance,self.min_variance,self.features_toremove,self.min_cat_percent,self.max_cat_percent,self.cat_values_toremove) = models[0]

    def fit(self,df):
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        df_numeric = df.select_dtypes(include=numerics)
        df_numeric = df_numeric.drop(self.exclude, axis=1)
        if self.use_variance and len(df_numeric.columns) > 0:
            selector = VarianceThreshold(self.min_variance)
            selector.fit(df_numeric)
            mask =  selector.get_support(False)
            names = []
            idx = 0
            for v in mask:
                if not v:
                    self.features_toremove.append(df_numeric.columns[idx])
                    idx += 1
        cat_cols = set(df.select_dtypes(exclude=numerics).columns)
        for col in cat_cols:
            cat_counts = df[col].value_counts(normalize=True,dropna=False)
            cat_to_remove = []
            for cat_val in cat_counts.index:
                if cat_counts[cat_val] < self.min_cat_percent:
                    cat_to_remove.append(cat_val)
            self.cat_values_toremove[col] = cat_to_remove
        print self.cat_values_toremove

    def transform(self,df):
        df.drop(self.features_toremove, axis=1, inplace=True)
        #loop over cols in cat_values_toremove and apply to nan each val
        return df    
                
