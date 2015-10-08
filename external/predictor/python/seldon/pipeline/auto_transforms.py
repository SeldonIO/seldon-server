import seldon.pipeline.pipelines as pl
from sklearn import preprocessing
from dateutil.parser import parse
import datetime
from collections import defaultdict
import numpy as np
import pandas as pd
import math

class Auto_transform(pl.Feature_transform):
    """Automatically transform a set of features into normalzied numeric or categorical features or dates

    Args:
        exclude (list):list of features to not include
    
        include (list): features to include if None then all unless exclude used
    
        max_values_numeric_categorical (int):max number of unique values for numeric feature to treat as categorical

        custom_date_formats (list(str)): list of custom date formats to try

        ignore_vals (list(str)): list of feature values to treat as NA/ignored values

        force_categorical (list(str)): features to force to be categorical
    """
    def __init__(self,exclude=[],include=None,max_values_numeric_categorical=0,date_cols=[],custom_date_formats=None,ignore_vals=None,force_categorical=[],min_cat_percent=0.0,max_cat_percent=1.0,bool_map={"true":1,"false":0,"1":1,"0":0,"yes":1,"no":0,"1.0":1,"0.0":0}):
        super(Auto_transform, self).__init__()
        self.exclude = exclude
        self.include = include
        self.max_values_numeric_categorical = max_values_numeric_categorical
        self.scalers = {}
        self.custom_date_formats = custom_date_formats
        if ignore_vals:
            self.ignore_vals = ignore_vals
        else:
            self.ignore_vals = ["NA",""]
        self.force_categorical = force_categorical
        self.catValueCount = {}
        self.convert_categorical = set()
        self.convert_date = set()
        self.date_cols = date_cols
        self.min_cat_percent = min_cat_percent
        self.max_cat_percent = max_cat_percent
        self.cat_percent = {}
        self.bool_map = bool_map
        self.convert_bool = set()

    def get_models(self):
        return [(self.exclude,self.include,self.custom_date_formats,self.max_values_numeric_categorical,self.force_categorical,self.ignore_vals,self.min_cat_percent,self.max_cat_percent),self.convert_categorical,self.convert_date,self.scalers,self.catValueCount,self.date_cols,self.cat_percent,self.bool_map,self.convert_bool]
    
    def set_models(self,models):
        (self.exclude,self.include,self.custom_date_formats,self.max_values_numeric_categorical,self.force_categorical,self.ignore_vals,self.min_cat_percent,self.max_cat_percent) = models[0]
        self.convert_categorical = models[1]
        self.convert_date = models[2]
        self.scalers = models[3]
        self.catValueCount = models[4]
        self.date_cols = models[5]
        self.cat_percent = models[6]
        self.bool_map = models[7]
        self.convert_bool = models[8]

    def to_date(self,f,v):
        d = None
        try:
            d = parse(v)
        except:
            for f in self.custom_date_formats:
                try:
                    d = datetime.datetime.strptime( v, f )
                except:
                    pass
        if d:
            return "t_"+str(int(self.unix_time(d)/86400))
        else:
            return None

    def scale(self,v,col):
        if np.isnan(v):
            return 0.0
        else:
            return self.scalers[col].transform([float(v)])[0]

    def make_cat(self,v,col):
        if not isinstance(v,basestring) and np.isnan(v):
            return v
        else:
            if col in self.cat_percent and v in self.cat_percent[col] and self.cat_percent[col][v] >= self.min_cat_percent and self.cat_percent[col][v] <= self.max_cat_percent:
                val = str(v)
                val = val.replace(" ","_").lower()
                return val
            else:
                return np.nan

    def create_hour_features(self,v,col):
        val = (v.hour/24.0) * 2*math.pi
        v1 = math.sin(val)
        v2 = math.cos(val)
        return pd.Series({col+"_"+'h1':v1, col+"_"+'h2':v2})

    def create_month_features(self,v,col):
        val = (v.month/12.0) * 2*math.pi
        v1 = math.sin(val)
        v2 = math.cos(val)
        return pd.Series({col+"_"+'m1':v1, col+"_"+'m2':v2})

    def create_dayofweek_features(self,v,col):
        val = (v.dayofweek/7.0) * 2*math.pi
        v1 = math.sin(val)
        v2 = math.cos(val)
        return pd.Series({col+"_"+'w1':v1, col+"_"+'w2':v2})

    def fit(self,df):
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        numeric_cols = set(df.select_dtypes(include=numerics).columns)
        categorical_cols = set(df.select_dtypes(exclude=numerics).columns)
        for col in df.columns:
            if col in self.exclude:
                pass
            elif not self.include or col in self.include:
                df[col].replace(self.ignore_vals,np.nan,inplace=True)
                df[col] = df[col].apply(lambda x: np.nan if isinstance(x, basestring) and len(x)==0 else x)
                cat_counts = df[col].value_counts(normalize=True)
                is_bool = True
                for val in cat_counts.index:
                    if not str(val).lower() in self.bool_map.keys():
                        is_bool = False
                        break
                if is_bool:
                    self.convert_bool.add(col)
                elif df[col].dtype in numerics:
                    if len(cat_counts) > self.max_values_numeric_categorical and not col in self.force_categorical:
                        print "fitting scaler for col ",col
                        dfs = df[col].dropna()
                        self.scalers[col] = preprocessing.StandardScaler(with_mean=True, with_std=True).fit(dfs.astype(float))
                    else:
                        self.convert_categorical.add(col)
                        self.cat_percent[col] = cat_counts
                else:
                    if df[col].dtype == 'datetime64[ns]':
                        self.convert_date.add(col)
                    elif col in self.date_cols:
                        self.convert_date.add(col)
                    else:
                        self.convert_categorical.add(col)
                        self.cat_percent[col] = cat_counts
        print "num scalers",len(self.scalers)
        print "num categorical ",len(self.convert_categorical)
        print "num dates",len(self.convert_date)
        print "num bool",len(self.convert_bool)

    def transform(self,df):
        c = 0
        num_bools  = len(self.convert_bool)
        for col in self.convert_bool:
            c += 1
            print "convert bool",col,c,"/",num_bools
            df[col] = df[col].apply(str).apply(str.lower)
            df[col] = df[col].map(self.bool_map)
        c = 0
        num_dates  = len(self.convert_date)
        for col in self.convert_date:
            c += 1
            print "convert date ",col,c,"/",num_dates
            if not df[col].dtype == 'datetime64[ns]':
                df[col] = pd.to_datetime(df[col])
                if not df[col].dtype == 'datetime64[ns]':
                    for f in self.custom_date_formats:
                        df[col] = pd.to_datetime(df[col],format=f)
                        if df[col].dtype == 'datetime64[ns]':
                            break
            if df[col].dtype == 'datetime64[ns]':
                df = pd.concat([df,df[col].apply(self.create_hour_features,col=col)],axis=1)
                df = pd.concat([df,df[col].apply(self.create_month_features,col=col)],axis=1)
                df = pd.concat([df,df[col].apply(self.create_dayofweek_features,col=col)],axis=1)
                df.drop(col,axis=1, inplace=True)
            else:
                print "warning - failed to convert to date col ",col
        c = 0
        num_cats = len(self.convert_categorical)
        for col in self.convert_categorical:
            df[col].replace(self.ignore_vals,np.nan,inplace=True)
            c += 1
            print "convert categorical ",col,c,"/",num_cats
            df[col] = df[col].apply(self.make_cat,col=col)
        num_scalers = len(self.scalers)
        c = 0
        for col in self.scalers:
            df[col].replace(self.ignore_vals,np.nan,inplace=True)
            c += 1
            print "scaling col ",col,c,"/",num_scalers
            df[col] = df[col].apply(self.scale,col=col)
        return df

