import seldon.fileutil as fu

class Feature_transform(object):

    def upload(self,fromPath,fromPath):
        fu.save(fromPath,toPath)
        
    def download(self,fromPath,toPath):
        fu.download(fromPath,toPath)

    def save(self,folder):
        print "no model to save"

    def load(self,folder):
        print "no model to load"


class Pipeline(object):

    def __init__(self,models_folder="./models"):
        self.pipeline = []
        self.models_folder = models_folder
        self.objs = []

    def add(self,feature_transform):
        self.pipeline.append(feature_transform)

    def process(self,line):
        j = json.loads(line)
        self.objs.append(j)

    def getFeatures(self,location):
        fu.stream(location,self.process)

    def transform(self,featureLocation):
        self.getFeatures(featuresLocation)
        for ft in pipeline:
            ft.load(self.models_folder)
            objs = ft.transform(objs)

    def fit_transform(self):
        self.getFeatures(featuresLocation)
        for ft in pipeline:
            ft.fit(objs)
            objs = ft.transform(objs)
            ft.save(self.models_folder)
