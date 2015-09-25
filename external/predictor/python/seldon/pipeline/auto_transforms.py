import seldon.pipeline.pipelines as pl
from sklearn import preprocessing
from dateutil.parser import parse
import datetime

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
    def __init__(self,exclude=[],include=None,max_values_numeric_categorical=20,custom_date_formats=None,ignore_vals=None,force_categorical=[]):
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
        self.transforms = {}
        self.force_categorical = force_categorical

    def get_models(self):
        return [(self.exclude,self.custom_date_formats,self.max_values_numeric_categorical),self.transforms,self.scalers]
    
    def set_models(self,models):
        (self.exclude,self.custom_date_formats,self.max_values_numeric_categorical) = models[0]
        self.transforms = models[1]
        self.scalers = models[2]

    @staticmethod
    def is_number(s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    @staticmethod
    def isBoolean(v):
        v = str(v)
        return v.lower() == "true" or v.lower() == "false" or v == "1" or v == "0"

    @staticmethod
    def toBoolean(f,v):
        v = str(v)
        if v.lower() == "true" or v == "1":
            return 1
        else:
            return 0

    def fit_scalers(self,objs,features):
        """fit numeric scalers on all numeric features

        requires enough memory to run sklearn standard scaler on all values for a feature
        """
        print "creating ",len(features),"features scalers"
        Xs = {}
        for f in features:
            Xs[f] = []
        for j in objs:
            for f in features:
                if f in j and self.is_number(j[f]):
                    Xs[f].append(float(j[f]))
        c = 1
        for f in Xs:
            print "creating feature scaler",c," for ",f
            self.scalers[f] = preprocessing.StandardScaler(with_mean=True, with_std=True).fit(Xs[f])
            c += 1

    def scale(self,f,v):
        if self.is_number(v):
            return self.scalers[f].transform([v])[0]
        else:
            return 0

    @staticmethod
    def make_categorical_token(f,v):
        """make a ctaegorical feature from feature and its value
        """
        v = str(v).lower().replace(" ","_")
        if Auto_transform.is_number(v):
            return f.replace(" ","_")+"_"+v
        else:
            return v

    def is_date(self,v):
        """is this feature a date
        """
        try:
            parse(v)
            return True
        except:
            if self.custom_date_formats:
                for f in self.custom_date_formats:
                    try:
                        datetime.datetime.strptime( v, f )
                        return True
                    except:
                        pass
            return False

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

    def fit(self,objs):
        """try to guess a transform to apply to each feature
        """
        values = {}
        c = 1
        for j in objs:
            for f in j:
                if f in self.exclude or j[f] in self.ignore_vals:
                    pass
                elif not self.include or f in self.include:
                    cur = values.get(f,set())
                    if len(cur) < (self.max_values_numeric_categorical + 1):
                        cur.add(j[f])
                        values[f] = cur
        featuresToScale = []
        for f in values:
            print f,values[f]
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

    def transform(self,j):
        """Apply learnt transforms on each feature
        """
        jNew = {}
        for f in j:
            if not f in self.transforms:
                jNew[f] = j[f]
            else:
                if not j[f] in self.ignore_vals:
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
