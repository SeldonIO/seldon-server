import seldon.pipeline.pipelines as pl
from collections import defaultdict
from  collections import OrderedDict
import logging
import operator
import re

class Include_features_transform(pl.Feature_transform):
    """Filter a dataset and include only specided set of features
    """
    def __init__(self,included=None):
        """filter dataset transform

        Args:
            included (list): list of features to be included
        """
        super(Include_features_transform, self).__init__()
        self.included = included


    def get_models(self):
        """get model data for this transform.
        """
        return [self.included]
    
    def set_models(self,models):
        """set the included features
        """
        self.included = models[0]
        self.logger.info("set feature names to %s",self.included)

    def fit(self,objs):
        """nothing to do in fit
        """
        pass

    def transform(self,j):
        """only include features specified in result
        """
        jNew = {}
        for feat in self.included:
            if feat in j:
                jNew[feat] = j[feat]
        return jNew


class Split_transform(pl.Feature_transform):
    """Split a set of string input features on an expression and create a new feature which has a list of values
    """
    def __init__(self,split_expression=" ",ignore_numbers=False,input_features=[]):
        """Split features

        Args:
            split_expression (str): regular expression to split feature on
            ignore_numbers (bool): whether to ignore any resulting strings that represent numbers
            input_features (list): list of feature names to split - should all have text values
        """
        super(Split_transform, self).__init__()
        self.split_expression=split_expression
        self.ignore_numbers=ignore_numbers
        self.input_features=input_features
        self.input_feature = ""

    def get_models(self):
        """return data needed for this transform
        """
        return super(Split_transform, self).get_models() + [self.split_expression,self.ignore_numbers,self.input_features]

    def set_models(self,models):
        """set model data
        """
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
        """transform text features by splitting them and creating a list of feature as result
        """
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
    """Filter rows based on whether a specified set of features exists
    """
    def __init__(self,included=None):
        """Filter feature rows

        Args:
            included (list): list of features that need to exist
        """
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
        """transform by returning input feature set if required features exist in it
        """
        ok = True
        for feat in self.included:
            if not feat in j:
                ok = False
        if ok:
            return j
        else:
            return None



class Svmlight_transform(pl.Feature_transform):
    """take a set of features and transform into a sorted dictionary of numeric id:value features
    """
    def __init__(self,included=None,zero_based=False,excluded=None):
        """create SVM light based feature

        Args:
            included (list): set of feature to use as input
            zero_based (bool): whether to start first id at 0
            excluded (list): set of features to exclude
        """
        super(Svmlight_transform, self).__init__()
        self.included = included
        self.excluded = excluded
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
        """create categorical or numeric feature.

        categorical features the id is created from input feature name and input (string) value with a final output value of 1
        """
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
        """create ids for each feature to be included
        """
        self.idMap = {}
        for j in objs:
            for f in j:
                if (self.included and f in self.included) or (self.excluded and not f in self.excluded):
                    v = j[f]
                    if isinstance(v, list):
                        for vList in v:
                            self.addId(f+"_"+vList,1)
                    elif isinstance(v,dict):
                        for k in v:
                            self.addId(f+"_"+k,v[k])
                    else:
                        self.addId(f,v)

    def transform(self,j):
        """transform features by getting id and numeric value
        """
        features = {}
        for f in j:
            if (self.included and f in self.included) or (self.excluded and not f in self.excluded):
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


class Feature_id_transform(pl.Feature_transform):
    """create a numeric feature id
    """
    def __init__(self,min_size=0,exclude_missing=False):
        """create numeric feature id

        Args:
            min_size (int): minimum number of examples of each feature value for feature to be included in transform as new id
            exclude_missing (bool): exclude rows that do not have the input feature
        """
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
        """create map of ids for each feature value

        create counts of occurrences of each feature value. Exclude features with not enough counds. Create id map.
        """
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
        """transform features creating a new id and exluding rows if needed
        """
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


