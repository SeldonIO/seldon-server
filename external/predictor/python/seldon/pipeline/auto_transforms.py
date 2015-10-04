import seldon.pipeline.pipelines as pl
from sklearn import preprocessing
from dateutil.parser import parse
import datetime
from collections import defaultdict

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
    def __init__(self,exclude=[],include=None,max_values_numeric_categorical=0,custom_date_formats=None,ignore_vals=None,force_categorical=[],min_categorical_keep_feature=0.0):
        super(Auto_transform, self).__init__()
        self.exclude = exclude
        self.include = include
        self.max_values_numeric_categorical = max_values_numeric_categorical
        self.scalers = {}
        self.custom_date_formats = custom_date_formats
        if ignore_vals:
            self.ignore_vals = set(ignore_vals)
        else:
            self.ignore_vals = set(["NA",""])
        self.force_categorical = force_categorical
        self.min_categorical_keep_feature = min_categorical_keep_feature
        self.catValueCount = {}
        self.convert_categorical = set()
        self.convert_date = set()

    def get_models(self):
        return [(self.exclude,self.include,self.custom_date_formats,self.max_values_numeric_categorical,self.force_categorical,self.min_categorical_keep_feature,self.ignore_vals),self.convert_categorical,self.scalers,self.catValueCount]
    
    def set_models(self,models):
        (self.exclude,self.include,self.custom_date_formats,self.max_values_numeric_categorical,self.force_categorical,self.min_categorical_keep_feature,self.ignore_vals) = models[0]
        self.convert_categorical = models[1]
        self.scalers = models[2]
        self.catValueCount = models[3]


    def unix_time(self,dt):
        """transform a date into a unix day number
        """
        epoch = datetime.datetime.utcfromtimestamp(0)
        delta = dt - epoch
        return delta.total_seconds()

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
        return self.scalers[col].transform([float(v)])[0]

    def make_cat(self,v,col):
        return col+"_"+str(v)

    def fit(self,df):
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        numeric_cols = set(df.select_dtypes(include=numerics).columns)
        categorical_cols = set(df.select_dtypes(exclude=numerics).columns)
        for col in df.columns:
            if col in self.exclude:
                pass
            elif not self.include or col in self.include:
                counts = df[col].value_counts()
                if col in numeric_cols:
                    if len(counts) > self.max_values_numeric_categorical:
                        print "fitting scaler for col ",col
                        self.scalers[col] = preprocessing.StandardScaler(with_mean=True, with_std=True).fit(df[col].astype(float))
                    else:
                        self.convert_categorical.add(col)
                else:
                    self.convert_categorical.add(col)

    def transform(self,df):
        for col in self.scalers:
          df[col] = df[col].apply(self.scale,col=col)
        for col in self.convert_categorical:
            df[col] = df[col].astype(str)
            df[col] = df[col].apply(self.make_cat,col=col)
        return df








    def _fit(self,objs):
        """try to guess a transform to apply to each feature
        """
        values = {}
        c = 0
        for j in objs:
            c += 1
            for f in j:
                if f in self.exclude or self.ignore_value(j[f]):
                    pass
                elif not self.include or f in self.include:
                    cur = values.get(f,set())
                    if len(cur) < (self.max_values_numeric_categorical + 1):
                        cur.add(j[f])
                        values[f] = cur
                        if not f in self.catValueCount:
                            self.catValueCount[f] = defaultdict(int)
                        self.catValueCount[f][j[f]] += 1
                    else:
                        if f in self.catValueCount:
                            del self.catValueCount[f]
        for f in self.catValueCount:
            for v in self.catValueCount[f]:
                self.catValueCount[f][v] /= float(c)
        featuresToScale = []
        for f in values:
            if f in self.force_categorical:
                self.transforms[f] = self.make_categorical_token.__name__
            elif all(self.isBoolean(x) for x in values[f]):
                self.transforms[f] = self.toBoolean.__name__
            else:
                if len(values[f]) > self.max_values_numeric_categorical:
                    if all(self.is_number(x) for x in values[f]):
                       featuresToScale.append(f)
                       self.transforms[f] = self.scale.__name__
                    elif all(self.is_date(x) for x in values[f]):
                        self.transforms[f] = self.to_date.__name__
                    else:
                        self.transforms[f] = self.make_categorical_token.__name__
                else:
                    self.transforms[f] = self.make_categorical_token.__name__
        self.fit_scalers(objs,featuresToScale)

    def _transform(self,j):
        """Apply learnt transforms on each feature
        """
        jNew = {}
        for f in j:
            if not f in self.transforms:
                jNew[f] = j[f]
            else:
                if not self.ignore_value(j[f]):
                    vNew = getattr(self,self.transforms[f])(f,j[f])
                    if vNew:
                        jNew[f] = vNew
        return jNew


if __name__ == '__main__':
    objs = [{"a":2.0,"b":"NA","c":1,"d":"29JAN14:21:16:00","e":46},{"a":2.0,"b":"false","c":"trousers","d":"31 jan 2015","e":46},{"a":1.0,"b":0,"c":"big hats","d":"28 aug 2015","e":46}]
    t = Auto_transform(max_values_numeric_categorical=1,custom_date_formats = ["%d%b%y:%H:%M:%S"])
    t.fit(objs)
    objsNew = []
    for j in objs:
        objsNew.append(t.transform(j))
    print objsNew
