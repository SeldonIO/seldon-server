import seldon.fileutil as fu
import json
from sklearn.externals import joblib
import os.path

class Feature_transform(object):

    def __init__(self):
        self.pos = 0
        self.input_feature = ""
        self.output_feature = ""

    def set_pipeline_pos(self,pos):
        self.pos = pos

    # return models
    def get_models(self):
        return [self.input_feature,self.output_feature]

    def set_models(self,models):
        self.input_feature = models[0]
        self.output_feature = models[1]
        return models[2:]

    def set_input_feature(self,feature):
        self.input_feature = feature

    def set_output_feature(self,feature):
        self.output_feature = feature

    def fit(self,objs):
        pass

    def transform(self,objs):
        return objs


class Pipeline(object):

    def __init__(self,input_folder=None,output_folder=None,models_folder=None,local_models_folder="./models",aws_key=None,aws_secret=None):
        self.pipeline = []
        self.models_folder = models_folder
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.local_models_folder = local_models_folder
        self.objs = []
        self.fu = fu.FileUtil(key=aws_key,secret=aws_secret)

    def full_class_name(self,o):
        return o.__class__.__module__ + "." + o.__class__.__name__

    def get_class(self, kls ):
        parts = kls.split('.')
        module = ".".join(parts[:-1])
        m = __import__( module )
        for comp in parts[1:]:
            m = getattr(m, comp)            
        return m

    def save_pipeline(self):
        f = open(self.local_models_folder+"/pipeline.txt","w")
        for t in self.pipeline:
            name = self.full_class_name(t)
            f.write(name+"\n")
        f.close()

    def load_pipeline(self):
        f =open(self.local_models_folder+"/pipeline.txt")
        for line in f:
            line = line.rstrip()
            cl = self.get_class(line)
            t = cl()
            self.add(t)
        f.close()

    #upload all files to models folder
    def upload_models(self):
        if self.models_folder:
            self.fu.copy(self.local_models_folder,self.models_folder)

    # download all files to local models folder
    def download_models(self):
        if self.models_folder:
            self.fu.copy(self.models_folder,self.local_models_folder)

    def store_features(self):
        if self.output_folder:
            local_file = "features.json"
            with open(local_file,"w") as f:
                for j in self.objs:
                    jStr =  json.dumps(j,sort_keys=True)
                    f.write(jStr+"\n")
            f.close()
            self.fu.copy(local_file,self.output_folder+"/features.json")

    def store_models(self):
        if not os.path.exists(self.local_models_folder):
            os.makedirs(self.local_models_folder)
        pos = 1
        for t in self.pipeline:
            models = t.get_models()
            joblib.dump(models,self.local_models_folder+"/"+str(pos))
            pos += 1

    def load_models(self):
        pos = 1
        for t in self.pipeline:
            models = joblib.load(self.local_models_folder+"/"+str(pos))
            t.set_models(models)
            pos += 1

    def add(self,feature_transform):
        self.pipeline.append(feature_transform)
        feature_transform.set_pipeline_pos(len(self.pipeline))

    def process(self,line):
        j = json.loads(line)
        self.objs.append(j)

    def getFeatures(self,location):
        print "streaming features from",location
        self.fu.stream(location,self.process)

    def transform(self):
        self.download_models()
        self.load_pipeline()
        self.load_models()
        self.getFeatures(self.input_folder)
        for ft in self.pipeline:
            self.objs = ft.transform(self.objs)
        return self.objs

    def fit_transform(self):
        self.getFeatures(self.input_folder)
        for ft in self.pipeline:
            ft.fit(self.objs)
            self.objs = ft.transform(self.objs)
        self.store_models()
        self.save_pipeline()
        self.upload_models()
        self.store_features()
        return self.objs



            
