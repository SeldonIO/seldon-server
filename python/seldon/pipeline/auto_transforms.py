from sklearn import preprocessing
from dateutil.parser import parse
import datetime
from collections import defaultdict
import numpy as np
import pandas as pd
import math
import itertools
from sklearn.base import BaseEstimator,TransformerMixin
import logging

logger = logging.getLogger(__name__)

class Auto_transform(BaseEstimator,TransformerMixin):
    """
    Automatically transform a set of features into normalzied numeric or categorical features or dates

    Parameters
    ----------

    exclude : list str, optional
       list of features to not include
    include : list str, optional
       features to include if None then all unless exclude used
    max_values_numeric_categorical : int, optional
       max number of unique values for numeric feature to treat as categorical
    custom_date_formats : list str, optional
       list of custom date formats to try
    ignore_vals : list str, optional
       list of feature values to treat as NA/ignored values
    force_categorical : list str, optional
       features to force to be categorical
    min_cat_percent : list str, optional
       min percentage for a categorical value to be kept
    max_cat_percent : float, optional
       max percentage for a categorical value to be kept
    bool_map : dict, optional
       set of string values to be treated as boolean
    cat_missing_value : str, optional
       string to use for missing categorical values
    date_transforms : list bool, optional
       which date transforms to apply [hour,month,day_of_week,year], default is all
    create_date_differences : bool, optional
       whether to create differences between all date variables
    nan_threshold : float, optional
       featurs to drop if too many nan, threshold is between 0-1 as percent
    drop_constant_features : bool, optional
       drop a column if its value is constant
    drop duplicate columns : bool, optional
       drop duplicate columns
    min_max_limit : bool, optional
       limit numeric cols to min and max seen in fit
    """
    def __init__(self,exclude=[],include=None,max_values_numeric_categorical=0,date_cols=[],custom_date_formats=None,ignore_vals=None,force_categorical=[],min_cat_percent=0.0,max_cat_percent=1.0,bool_map={"true":1,"false":0,"1":1,"0":0,"yes":1,"no":0,"1.0":1,"0.0":0},cat_missing_val="UKN",date_transforms=[True,True,True,True],create_date_differences=False,nan_threshold=None,drop_constant_features=True,drop_duplicate_cols=True,min_max_limit=False):
        super(Auto_transform, self).__init__()
        self.exclude = exclude
        self.include = include
        self.max_values_numeric_categorical = max_values_numeric_categorical
        self.scalers = {}
        self.date_diff_scalers = {}
        self.custom_date_formats = custom_date_formats
        self.ignore_vals  = ignore_vals
        self.force_categorical = force_categorical
        self.catValueCount = {}
        self.convert_categorical = []
        self.convert_date = []
        self.date_cols = date_cols
        self.min_cat_percent = min_cat_percent
        self.max_cat_percent = max_cat_percent
        self.cat_percent = {}
        self.bool_map = bool_map
        self.convert_bool = []
        self.cat_missing_val = cat_missing_val
        self.date_transforms=date_transforms
        self.create_date_differences = create_date_differences
        self.nan_threshold=nan_threshold
        self.drop_cols = []
        self.drop_constant_features=drop_constant_features
        self.drop_duplicate_cols=drop_duplicate_cols
        self.min_max_limit=min_max_limit
        self.min_max = {}

    def _scale(self,v,col):
        if np.isnan(v):
            return 0.0
        else:
            return self.scalers[col].transform([[float(v)]])[0,0]

    def _scale_date_diff(self,v,col):
        if np.isnan(v):
            return 0.0
        else:
            return self.date_diff_scalers[col].transform([[float(v)]])[0,0]

    @staticmethod
    def _is_number(s):
        try:
            float(s)
            return True
        except ValueError:
            return False


    def _make_cat(self,v,col):
        if not isinstance(v,basestring) and np.isnan(v):
            return self.cat_missing_val
        else:
            if col in self.cat_percent and v in self.cat_percent[col] and self.cat_percent[col][v] >= self.min_cat_percent and self.cat_percent[col][v] <= self.max_cat_percent:
                val = unicode(str(v), "utf-8")
                if self._is_number(v):
                    val = col + "_" + val.replace(" ","_").lower()
                else:
                    val = val.replace(" ","_").lower()
                return val
            else:
                return np.nan

    def _create_hour_features(self,v,col):
        val = (v.hour/24.0) * 2*math.pi
        v1 = math.sin(val)
        v2 = math.cos(val)
        return pd.Series({col+"_hour":"h"+str(v.hour),col+"_"+'h1':v1, col+"_"+'h2':v2})

    def _create_month_features(self,v,col):
        val = (v.month/12.0) * 2*math.pi
        v1 = math.sin(val)
        v2 = math.cos(val)
        return pd.Series({col+"_month":"m"+str(v.month),col+"_"+'m1':v1, col+"_"+'m2':v2})

    def _create_dayofweek_features(self,v,col):
        val = (v.dayofweek/7.0) * 2*math.pi
        v1 = math.sin(val)
        v2 = math.cos(val)
        return pd.Series({col+"_w":"w"+str(v.dayofweek),col+"_"+'w1':v1, col+"_"+'w2':v2})

    def _create_year_features(self,v,col):
        return pd.Series({col+"_year":"y"+str(v.year)})


    def _convert_to_date(self,df,col):
        if not df[col].dtype == 'datetime64[ns]':
            try:
                return pd.to_datetime(df[col])
            except:
                logger.info("failed default conversion ")
                pass
            for f in self.custom_date_formats:
                try:
                    return pd.to_datetime(df[col],format=f)
                except:
                    logger.info("failed custom conversion %s",f)
                    pass
            return None
        else:
            return df[col]

    def _duplicate_columns(self,frame):
        groups = frame.columns.to_series().groupby(frame.dtypes).groups
        dups = []
        for t, v in groups.items():
            dcols = frame[v].to_dict(orient="list")

            vs = dcols.values()
            ks = dcols.keys()
            lvs = len(vs)

            for i in range(lvs):
                for j in range(i+1,lvs):
                    if vs[i] == vs[j]: 
                        dups.append(ks[i])
                        break

        return dups       



    def fit(self,df):
        """
        Fit models against an input pandas dataframe

        Parameters
        ----------

        X : pandas dataframe 

        Returns
        -------
        self: object

        """
        if not self.nan_threshold is None:
            max_nan = float(len(df)) * self.nan_threshold
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        numeric_cols = set(df.select_dtypes(include=numerics).columns)
        categorical_cols = set(df.select_dtypes(exclude=numerics).columns)
        if self.drop_duplicate_cols:
            self.drop_cols = self._duplicate_columns(df)
            logger.info("Adding duplicate cols to be dropped %s",self.drop_cols)
        for col in df.columns:
            if col in self.exclude:
                continue
            if col in self.drop_cols:
                continue
            elif not self.include or col in self.include:
                if not self.nan_threshold is None:
                    num_nan = len(df) - df[col].count()
                    if num_nan > max_nan:
                        if not col in self.drop_cols:
                            logger.info("adding %s to drop columns %d %d",col,num_nan,max_nan)
                            self.drop_cols.append(col)
                            continue
                if not self.ignore_vals is None:
                    df[col].replace(self.ignore_vals,np.nan,inplace=True)
                df[col] = df[col].apply(lambda x: np.nan if isinstance(x, basestring) and len(x)==0 else x)
                cat_counts = df[col].value_counts(normalize=True,dropna=False)
                if len(cat_counts) == 1 and self.drop_constant_features:
                    if not col in self.drop_cols:
                        logger.info("adding %s to drop columns as is constant",col)
                        self.drop_cols.append(col)
                        continue
                is_bool = True
                for val in cat_counts.index:
                    if not str(val).lower() in self.bool_map.keys():
                        is_bool = False
                        break
                if is_bool:
                    self.convert_bool.append(col)
                elif df[col].dtype in numerics:
                    if len(cat_counts) > self.max_values_numeric_categorical and not col in self.force_categorical:
                        logger.info("fitting scaler for col %s",col)
                        dfs = df[col].dropna()
                        if dfs.shape[0] > 0:
                            arr = dfs.astype(float).values.reshape(-1,1)
                            self.scalers[col] = preprocessing.StandardScaler(with_mean=True, with_std=True).fit(arr)
                            self.min_max[col] = (dfs.min(),dfs.max())
                    else:
                        self.convert_categorical.append(col)
                        self.cat_percent[col] = cat_counts
                else:
                    if df[col].dtype == 'datetime64[ns]':
                        self.convert_date.append(col)
                    elif col in self.date_cols:
                        self.convert_date.append(col)
                    else:
                        self.convert_categorical.append(col)
                        self.cat_percent[col] = cat_counts
        if self.create_date_differences:
            dates_converted = pd.DataFrame([])
            for col in self.convert_date:
                date_converted = self._convert_to_date(df,col)
                if not date_converted is None:
                    dates_converted[col] = date_converted
            if len(dates_converted.columns)>1:
                for (col1,col2) in itertools.combinations(dates_converted.columns, 2):
                    logger.info("training date diff scaler for %s %s",col1,col2)
                    d_diff = dates_converted[col1] - dates_converted[col2]
                    d_diff = (d_diff / np.timedelta64(1, 'D')).astype(float)
                    self.date_diff_scalers[col1+"_"+col2] = preprocessing.StandardScaler(with_mean=True, with_std=True).fit(arr)
        logger.info("num columns to drop %d",len(self.drop_cols))
        logger.info("num scalers %d",len(self.scalers))
        logger.info("num categorical %d",len(self.convert_categorical))
        logger.info("num dates %d",len(self.convert_date))
        logger.info("num date diffs %d",len(self.date_diff_scalers))
        logger.info("num bool %d",len(self.convert_bool))
        return self

    def transform(self,df):
        """
        transform a datframe with fitted models

        Parameters
        ----------

        X : pandas dataframe 

        Returns
        -------
        
        Transformed pandas dataframe

        """
        df = df.drop(self.drop_cols,axis=1)
        c = 0
        num_bools  = len(self.convert_bool)
        for col in self.convert_bool:
            c += 1
            logger.info("convert bool %s %d/%d",col,c,num_bools)
            df[col] = df[col].apply(str).apply(str.lower)
            df[col] = df[col].map(self.bool_map)
        c = 0
        num_dates  = len(self.convert_date)
        dates_converted = []
        for col in self.convert_date:
            c += 1
            logger.info("convert date %s %d/%d %s",col,c,num_dates,df[col].dtype)
            date_converted = self._convert_to_date(df,col)
            if not date_converted is None:
                logger.info("successfully converted %s to date",col)
                df[col] = date_converted
                dates_converted.append(col)
            if df[col].dtype == 'datetime64[ns]':
                if self.date_transforms[0]:
                    logger.info("creating hour features")
                    df = pd.concat([df,df[col].apply(self._create_hour_features,col=col)],axis=1)
                if self.date_transforms[1]:
                    logger.info("creating month features")
                    df = pd.concat([df,df[col].apply(self._create_month_features,col=col)],axis=1)
                if self.date_transforms[2]:                    
                    logger.info("creating day of week features")
                    df = pd.concat([df,df[col].apply(self._create_dayofweek_features,col=col)],axis=1)
                if self.date_transforms[3]:                    
                    logger.info("creating year features")
                    df = pd.concat([df,df[col].apply(self._create_year_features,col=col)],axis=1)
            else:
                logger.info("warning - failed to convert to date col %s",col)
        if self.create_date_differences and len(dates_converted) > 1:
            for (col1,col2) in itertools.combinations(dates_converted, 2):
                logger.info("diff scaler for %s %s",col1,col2)
                col_name = col1+"_"+col2
                df[col_name] = df[col1] - df[col2]
                df[col_name] = (df[col_name] / np.timedelta64(1, 'D')).astype(float)
                if not self.ignore_vals is None:
                    df[col_name].replace(self.ignore_vals,np.nan,inplace=True)
                df[col_name] = df[col_name].apply(self._scale_date_diff,col=col_name)
        c = 0
        num_cats = len(self.convert_categorical)
        for col in self.convert_categorical:
            if not self.ignore_vals is None:
                df[col].replace(self.ignore_vals,np.nan,inplace=True)
            c += 1
            logger.info("convert categorical %s %d/%d ",col,c,num_cats)
            df[col] = df[col].apply(self._make_cat,col=col)
        num_scalers = len(self.scalers)
        c = 0
        for col in self.scalers:
            if not self.ignore_vals is None:
                df[col].replace(self.ignore_vals,np.nan,inplace=True)
            if self.min_max_limit:
                df[col] = df[col].apply(lambda x : self.min_max[col][0] if x < self.min_max[col][0] else x)
                df[col] = df[col].apply(lambda x : self.min_max[col][1] if x > self.min_max[col][1] else x)
            c += 1
            logger.info("scaling col %s %d/%d",col,c,num_scalers)
            df[col] = df[col].apply(self._scale,col=col)
        return df

