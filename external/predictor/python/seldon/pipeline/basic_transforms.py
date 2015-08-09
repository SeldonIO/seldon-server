import seldon.pipeline.pipelines as pl

class Include_features_transform(pl.Feature_transform):

    def __init__(self,included=None):
        self.included = included

    def get_models(self):
        return [self.included]
    
    def get_model_names(self):
        return [self.__class__.__name__+"_included"]

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
                jNew[feat] = j[feat]
            objs_new.append(jNew)
        return objs_new



