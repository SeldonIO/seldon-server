import seldon.fileutil as fu
import json
from sklearn.externals import joblib

class Feature_transform(object):

    # return models
    def get_models(self):
        return [self.input_feature,self.output_feature]
    
    # return model string names
    def get_model_names(self):
        return [self.__class__.__name__+"_input_feature",self.__class__.__name__+"_output_feature"]

    def set_models(self,models):
        self.input_feature = models[0]
        self.output_feature = models[1]
        return models[2:]

    def set_input_feature(self,feature):
        self.input_feature = feature

    def set_output_feature(self,feature):
        self.output_feature = feature


class Pipeline(object):

    def __init__(self,input_folder=None,output_folder=None,models_folder=None,local_models_folder="./models",aws_key=None,aws_secret=None):
        self.pipeline = []
        self.models_folder = models_folder
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.local_models_folder = local_models_folder
        self.objs = []
        self.fu = fu.FileUtil(key=aws_key,secret=aws_secret)

    #upload all files to models folder
    def upload_models(self):
        if self.models_folder:
            self.fu.copy(self.local_models_folder,self.models_folder)

    # download all files to local models folder
    def download_models(self):
        if self.models_folder:
            self.fu.copy(self.models_folder,self.local_models_folder)

    def store_features(self):
        local_file = "features.json"
        with open(local_file,"w") as f:
            for j in self.objs:
                jStr =  json.dumps(j,sort_keys=True)
                f.write(jStr+"\n")
        f.close()
        self.fu.copy(local_file,self.output_folder+"/features.json")

    def store_model(self,model,name):
        joblib.dump(model,self.local_models_folder+"/"+name)

    def load_models(self,names):
        models = []
        for name in names:
            models.append(joblib.load(self.local_models_folder+"/"+name))
        return models

    def add(self,feature_transform):
        self.pipeline.append(feature_transform)

    def process(self,line):
        j = json.loads(line)
        self.objs.append(j)

    def getFeatures(self,location):
        print "streaming features from",location
        self.fu.stream(location,self.process)

    def transform(self):
        self.download_models()
        self.getFeatures(self.input_folder)
        for ft in self.pipeline:
            ft.set_models(self.load_models(ft.get_model_names()))
            self.objs = ft.transform(self.objs)
        return self.objs

    def fit_transform(self):
        self.getFeatures(self.input_folder)
        for ft in self.pipeline:
            ft.fit(self.objs)
            self.objs = ft.transform(self.objs)
            for (model,name) in zip(ft.get_models(),ft.get_model_names()):
                self.store_model(model,name)
        self.upload_models()
        self.store_features()
        return self.objs



            
