import seldon.pipeline.pipelines as pl
from collections import defaultdict
from  collections import OrderedDict
import logging
import operator
import re

class Include_features_transform(pl.Feature_transform):

    def __init__(self,included=None):
        super(Include_features_transform, self).__init__()
        self.included = included


    def get_models(self):
        return [self.included]
    
    def set_models(self,models):
        self.included = models[0]
        self.logger.info("set feature names to %s",self.included)

    def fit(self,objs):
        pass

    def transform(self,j):
        jNew = {}
        for feat in self.included:
            if feat in j:
                jNew[feat] = j[feat]
        return jNew


class Split_transform(pl.Feature_transform):

    def __init__(self,split_expression=" ",ignore_numbers=False,input_features=[]):
        super(Split_transform, self).__init__()
        self.split_expression=split_expression
        self.ignore_numbers=ignore_numbers
        self.input_features=input_features
        self.input_feature = ""

    def get_models(self):
        return super(Split_transform, self).get_models() + [self.split_expression,self.ignore_numbers,self.input_features]

    def set_models(self,models):
        models = super(Split_transform, self).set_models(models)
        self.split_expression = models[0]
        self.ignore_features= models[1]
        self.input_features = models[2]

    def is_number(self,s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def fit(self,objs):
        pass

    def transform(self,j):
        ftokens = []
        for feat in self.input_features:
            if feat in j:
                tokens = re.split(self.split_expression,j[feat])
                for token in tokens:
                    token = token.rstrip().lower()
                    if not self.ignore_numbers or (self.ignore_numbers and not self.is_number(token)):
                        ftokens.append(token)
        j[self.output_feature] = ftokens
        return j

class Exist_features_transform(pl.Feature_transform):

    def __init__(self,included=None):
        super(Exist_features_transform, self).__init__()
        self.included = included

    def get_models(self):
        return [self.included]

    def set_models(self,models):
        self.included = models[0]
        self.logger.info("%s set feature names to %s",self.get_log_prefix(),self.included)

    def fit(self,objs):
        pass

    def transform(self,j):
        ok = True
        for feat in self.included:
            if not feat in j:
                ok = False
        if ok:
            return j
        else:
            return None


# Take a set of features and transform into a sorted dictionary of id:value features
class Svmlight_transform(pl.Feature_transform):

    def __init__(self,included=None,zero_based=False,excluded=None):
        super(Svmlight_transform, self).__init__()
        self.included = included
        self.excluded = None
        self.idMap = {}
        self.zero_based = False
        if self.zero_based:
            self.lastId = 0
        else:
            self.lastId = 1

    
    def get_models(self):
        return super(Svmlight_transform, self).get_models() + [(self.included,self.excluded),self.idMap]
    
    def set_models(self,models):
        models = super(Svmlight_transform, self).set_models(models)
        (self.included,self.excluded) = models[0]
        self.idMap = models[1]

    @staticmethod
    def is_number(s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def get_feature_val(self,feature,val):
        if self.is_number(val):
            return (feature,float(val))
        else:
            return (feature+"_"+val,1)

    def addId(self,feature,val):
        (f,v) = self.get_feature_val(feature,val)
        if not f in self.idMap:
            self.idMap[f] = self.lastId
            self.lastId += 1

    def getId(self,feature,val):
        (f,v) = self.get_feature_val(feature,val)
        if not f in self.idMap:
            return (None,0)
        else:
            return (self.idMap[f],v)


    def fit(self,objs):
        self.idMap = {}
        for j in objs:
            for f in self.included:
                v = j[f]
                if isinstance(v, list):
                    for vList in cl:
                        self.addId(f+"_"+vList,1)
                elif isinstance(v,dict):
                    for k in v:
                        self.addId(f+"_"+k,v[k])
                else:
                    self.addId(f,v)

    def transform(self,j):
        features = {}
        for f in self.included:
            v = j[f]
            if isinstance(v, list):
                for vList in cl:
                    (fid,fv) = self.getId(f+"_"+vList,1) 
                    if fid:
                        features[fid] = fv
            elif isinstance(v,dict):
                for k in v:
                    (fid,fv) = self.getId(f+"_"+k,v[k])
                    if fid:
                        features[fid] = fv
            else:
                (fid,fv) = self.getId(f,v)
                if fid:
                    features[fid] = fv
        od = OrderedDict(sorted(features.items()))
        j[self.output_feature] = od
        return j


# Add a feature id feature
# can parse a literal or a list of values into ids
# optionally filter by class size
class Feature_id_transform(pl.Feature_transform):

    def __init__(self,min_size=0,exclude_missing=False):
        super(Feature_id_transform, self).__init__()
        self.min_size = min_size
        self.exclude_missing = exclude_missing
        self.idMap = {}

    def get_models(self):
        return super(Feature_id_transform, self).get_models() + [(self.min_size,self.exclude_missing),self.idMap]
    
    def set_models(self,models):
        models = super(Feature_id_transform, self).set_models(models)
        (self.min_size,self.exclude_missing) = models[0]
        self.logger.info("%s set min feature size to %d ",self.get_log_prefix(),self.min_size)
        self.logger.info("%s exclude missing to %s ",self.get_log_prefix(),self.exclude_missing)
        self.idMap = models[1]


    def fit(self,objs):
        sizeMap = defaultdict(int)
        for j in objs:
            if self.input_feature in j:
                cl = j[self.input_feature]
                if isinstance(cl, list):
                    for v in cl:
                        sizeMap[v] += 1
                else:
                    sizeMap[cl] += 1
        sorted_x = sorted(sizeMap.items(), key=operator.itemgetter(1),reverse=True)        
        nxtId = 1
        for (feature,size) in sorted_x:
            if size > self.min_size:
                self.idMap[feature] = nxtId
                nxtId += 1
        self.logger.info("%s Final id map has size %d",self.get_log_prefix(),len(self.idMap))

    def transform(self,j):
        exclude = False
        if self.input_feature in j:
            cl = j[self.input_feature]
            if isinstance(cl, list):
                r = []
                for v in cl:
                    if v in self.idMap:
                        r.append(self.idMap[v])
                j[self.output_feature] = r
            else:
                if cl in self.idMap:
                    j[self.output_feature] = self.idMap[cl]
                elif self.exclude_missing:
                    exclude = True
        if not exclude:
            return j
        else:
            return None


