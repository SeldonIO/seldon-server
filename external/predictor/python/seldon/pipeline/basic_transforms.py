import seldon.pipeline.pipelines as pl
from collections import defaultdict
import logging
import operator

class Include_features_transform(pl.Feature_transform):

    def __init__(self,included=None):
        self.included = included

    def get_models(self):
        return [self.included]
    
    def get_model_names(self):
        return ["included"]

    def set_models(self,models):
        self.included = models[0]
        print "set feature names to ",self.included

    def fit(self,objs):
        pass

    def transform(self,objs):
        objs_new = []
        for j in objs:
            jNew = {}
            for feat in self.included:
                if feat in j:
                    jNew[feat] = j[feat]
            objs_new.append(jNew)
        return objs_new

class Exist_features_transform(pl.Feature_transform):

    def __init__(self,included=None):
        self.included = included
        self.logger = logging.getLogger('seldon')

    def get_models(self):
        return [self.included]
    
    def get_model_names(self):
        return ["included"]

    def set_models(self,models):
        self.included = models[0]
        print "set feature names to ",self.included

    def fit(self,objs):
        pass

    def transform(self,objs):
        objs_new = []
        excluded = 0
        for j in objs:
            ok = True
            for feat in self.included:
                if not feat in j:
                    ok = False
            if ok:
                objs_new.append(j)
            else:
                excluded += 1
        print "excluded ",excluded
        return objs_new

# split a feature into a list of tokens
class Split_feature_transform(pl.Feature_transform):

    def __init__(self,separator=" "):
        self.separator = separator

    def get_models(self):
        return super(Split_feature_transform, self).get_models() + [self.separator]
    
    def get_model_names(self):
        return super(Split_feature_transform, self).get_model_names() + ["separator"]

    def set_models(self,models):
        models = super(Split_feature_transform, self).set_models(models)
        self.separator = models[0]

    def fit(self,objs):
        pass

    def transform(self,objs):
        objs_new = []
        for j in objs:
            if self.input_feature in j:
                j[self.output_feature] = j[self.input_feature].split(self.separator)
            objs_new.append(j)
        return objs_new


# Add a feature id feature
# can parse a literal or a list of values into ids
# optionally filter by class size
class Feature_id_transform(pl.Feature_transform):

    def __init__(self,min_size=0,exclude_missing=False):
        self.min_size = min_size
        self.exclude_missing = exclude_missing
        self.idMap = {}
        self.logger = logging.getLogger('seldon')

    def get_models(self):
        return super(Feature_id_transform, self).get_models() + [(self.min_size,self.exclude_missing),self.idMap]
    
    def get_model_names(self):
        return super(Feature_id_transform, self).get_model_names() + ["params","idMap"]

    def set_models(self,models):
        models = super(Feature_id_transform, self).set_models(models)
        (self.min_size,self.exclude_missing) = models[0]
        self.logger.info("set min feature size to %d ",self.min_size)
        self.logger.info("exclude missing to %s ",self.exclude_missing)
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
        self.logger.info("Final id map has size %d",len(self.idMap))

    def transform(self,objs):
        objs_new = []
        for j in objs:
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
            elif self.exclude_missing:
                exclude = True
            if not exclude:
                objs_new.append(j)
        return objs_new


