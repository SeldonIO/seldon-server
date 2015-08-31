import seldon.fileutil as fu
import json
from sklearn.externals import joblib
import os.path
import logging
import shutil 
import unicodecsv

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

    def get_added_features(self):
        if self.output_feature:
            return [self.output_feature]
        else:
            return []
            

    def set_input_feature(self,feature):
        self.input_feature = feature

    def set_output_feature(self,feature):
        self.output_feature = feature

    def fit(self,objs):
        pass

    def transform(self,obj):
        return obj

    def set_logging(self,logger):
        self.logger = logger

    def get_log_prefix(self):
        return self.__class__.__module__ + "." + self.__class__.__name__+" "+self.input_feature+"->"+self.output_feature

class Pipeline(object):

    def __init__(self,input_folders=[],output_folder=None,models_folder=None,local_models_folder="./models",local_data_folder="./data",aws_key=None,aws_secret=None,data_type='json'):
        self.pipeline = []
        self.models_folder = models_folder
        self.input_folders = input_folders
        self.output_folder = output_folder
        self.local_models_folder = local_models_folder
        self.local_data_folder = local_data_folder
        if not os.path.exists(self.local_models_folder):
            os.makedirs(self.local_models_folder)
        if not os.path.exists(self.local_data_folder):
            os.makedirs(self.local_data_folder)
        self.fu = fu.FileUtil(key=aws_key,secret=aws_secret)
        self.logger = logging.getLogger('seldon')
        self.logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.current_dataset = self.local_data_folder + "/current"
        self.next_dataset = self.local_data_folder + "/next"
        self.data_type = data_type

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
            self.fu.copy(self.current_dataset,self.output_folder+"/features.json")

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
        feature_transform.set_logging(self.logger)

    def save_features_local(self,line):
        self.active_file.write(line+"\n")


    def getFeatures(self,locations):
        print "streaming features from",locations," to ",self.current_dataset
        self.active_file = open(self.current_dataset,"w")
        self.fu.stream_multi(locations,self.save_features_local)
        self.active_file.close()

    def transform_init(self):
        self.download_models()
        self.load_pipeline()
        self.load_models()

    def transform_json(self,json):
        for ft in self.pipeline:
            json = ft.transform(json)
        return json

    def get_csv_fieldnames(self,ds,ft):
        fieldnames = set(ft.get_added_features())
        for d in ds:
            fieldnames = fieldnames.union(set(d.keys()))
            break
        return list(fieldnames)

    def _transform(self,ds,ft):
        self.active_file = open(self.next_dataset,"w")
        if self.data_type == 'csv':
            csvwriter = unicodecsv.DictWriter(self.active_file, fieldnames=self.get_csv_fieldnames(ds,ft), restval="NA")
            csvwriter.writeheader()
        for j in ds:
            jNew = ft.transform(j)
            if jNew:
                if self.data_type == 'csv':
                    csvwriter.writerow(jNew)
                else:
                    jStr =  json.dumps(jNew,sort_keys=True)
                    self.active_file.write(jStr+"\n")
        self.active_file.close()
        shutil.move(self.next_dataset,self.current_dataset)

    def get_dataset(self,filename):
        if self.data_type == "json":
            return JsonDataSet(filename)
        elif self.data_type == "csv":
            return CsvDataSet(filename)

    def transform(self):
        self.transform_init()
        self.getFeatures(self.input_folders)
        for ft in self.pipeline:
            ds = self.get_dataset(self.current_dataset)
            self._transform(ds,ft)
        return self.get_dataset(self.current_dataset)

    def fit_transform(self):
        self.getFeatures(self.input_folders)
        for ft in self.pipeline:
            ds = self.get_dataset(self.current_dataset)
            ft.fit(ds)
            self._transform(ds,ft)
        self.store_models()
        self.save_pipeline()
        self.upload_models()
        self.store_features()
        return self.get_dataset(self.current_dataset)

class JsonDataSet(object):

    def __init__(self, filename):
        self.filename =filename
 
    def __iter__(self):
        f = open(self.filename,"r")
        for line in f:
            line = line.rstrip()
            j = json.loads(line)
            yield j

class CsvDataSet(object):

    def __init__(self, filename):
        self.filename =filename
 
    def __iter__(self):
        csvfile = open(self.filename,"r")
        reader = unicodecsv.DictReader(csvfile,encoding='utf-8')
        for d in reader:
            yield d


            
