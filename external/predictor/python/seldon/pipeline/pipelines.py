import seldon.fileutil as fu
import json
from sklearn.externals import joblib
import os.path
import logging
import shutil 
import unicodecsv

class Feature_transform(object):
    """Base feature transformation class with method defaults
    """
    def __init__(self):
        self.pos = 0
        self.input_feature = ""
        self.output_feature = ""

    def set_pipeline_pos(self,pos):
        """Store position in pipeline for this transformation
        """
        self.pos = pos

    def get_models(self):
        """return the input and output features
        """
        return [self.input_feature,self.output_feature]

    def set_models(self,models):
        """set the input and output features and return rest of models info to caller
        """
        self.input_feature = models[0]
        self.output_feature = models[1]
        return models[2:]

    def get_added_features(self):
        """return the feature added by this transformation

        This is used for CSV files where we need to know CSV column names at start
        """
        if self.output_feature:
            return [self.output_feature]
        else:
            return []
            

    def set_input_feature(self,feature):
        """setter for input feature
        """
        self.input_feature = feature

    def set_output_feature(self,feature):
        """ setter for output feature
        """
        self.output_feature = feature

    def fit(self,objs):
        """fir method by default does nothing
        """
        pass

    def transform(self,obj):
        """transform by default does nothing
        """
        return obj

    def set_logging(self,logger):
        """setter for logging 
        """
        self.logger = logger

    def get_log_prefix(self):
        """get a useful description for logging
        """
        return self.__class__.__module__ + "." + self.__class__.__name__+" "+self.input_feature+"->"+self.output_feature

class Pipeline(object):
    """A pipeline of feature transformation

    Args:
        input_folders (list): list of folders containing feature files (or single file for CSV)

        output_folder [Optional(str)]: output folder to store the result of the pipeline

        models_folder [Optional(str)]: folder to store the feature transformation models needed to rerun the pipeline

        local_models_folder (str): local tmp folder to store models while processing

        local_data_folder (str): local tmp data folder to store data while processing

        aws_key [Optional(str)]: aws key (for S3 access)

        aws_secret [Optional(str)]: aws secret (for S3 access)

        data_type (str): json or csv
    """

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
        """get name of class

        Args:
            o (object): class instance to get name for
        """
        return o.__class__.__module__ + "." + o.__class__.__name__

    def get_class(self, kls ):
        """get a named class

        Args:
            kls (str): class name
        """
        parts = kls.split('.')
        module = ".".join(parts[:-1])
        m = __import__( module )
        for comp in parts[1:]:
            m = getattr(m, comp)            
        return m

    def save_pipeline(self):
        """save a pipeline

        store ordered list of class names
        """
        f = open(self.local_models_folder+"/pipeline.txt","w")
        for t in self.pipeline:
            name = self.full_class_name(t)
            f.write(name+"\n")
        f.close()

    def load_pipeline(self):
        """load a pipeline

        create an instance of each class specified in pipeline defns file and add to pipeline
        """
        f =open(self.local_models_folder+"/pipeline.txt")
        for line in f:
            line = line.rstrip()
            cl = self.get_class(line)
            t = cl()
            self.add(t)
        f.close()

    def upload_models(self):
        """upload models from local folder to final models folder
        """
        if self.models_folder:
            self.fu.copy(self.local_models_folder,self.models_folder)

    def download_models(self):
        """download models to local folder
        """
        if self.models_folder:
            self.fu.copy(self.models_folder,self.local_models_folder)

    def store_features(self):
        """store features in final location folder
        """
        if self.output_folder:
            self.fu.copy(self.current_dataset,self.output_folder+"/features")

    def store_models(self):
        """for each transformation in pipeline get models and pickle them
        """
        if not os.path.exists(self.local_models_folder):
            os.makedirs(self.local_models_folder)
        pos = 1
        for t in self.pipeline:
            models = t.get_models()
            joblib.dump(models,self.local_models_folder+"/"+str(pos))
            pos += 1

    def load_models(self):
        """local models for each transformation in pipelie from pickle file
        """
        pos = 1
        for t in self.pipeline:
            models = joblib.load(self.local_models_folder+"/"+str(pos))
            t.set_models(models)
            pos += 1

    def add(self,feature_transform):
        """add a transformation to a pipeline

        Args:
            feature_transform (object): instance of feature transform class
        """
        self.pipeline.append(feature_transform)
        feature_transform.set_pipeline_pos(len(self.pipeline))
        feature_transform.set_logging(self.logger)

    def save_features_local(self,line):
        """save data line to local features file

        Args:
            line (str): features data line
        """
        self.active_file.write(line+"\n")


    def getFeatures(self,locations):
        """get features from folders and store locally the data

        Args:
            locations (list): list of folders
        """
        print "streaming features from",locations," to ",self.current_dataset
        self.active_file = open(self.current_dataset,"w")
        self.fu.stream_multi(locations,self.save_features_local)
        self.active_file.close()

    def transform_init(self):
        """prepare for transform

        download models locally, load the pipeline and set its models
        """
        self.download_models()
        self.load_pipeline()
        self.load_models()

    def transform_json(self,f):
        """transform a features dictionary row

        Args:
            f (dict): features to transform
        """
        for ft in self.pipeline:
            f = ft.transform(f)
        return f

    def get_csv_fieldnames(self,ds,ft):
        """get the fieldnames for csv output for thsi transform

        union of added features from transform along with existing features

        Args:
            ds (Dataset): dataset
            ft (Feature_transform): feature transform
        """
        fieldnames = set(ft.get_added_features())
        for d in ds:
            fieldnames = fieldnames.union(set(d.keys()))
            break
        return list(fieldnames)

    def _transform(self,ds,ft):
        """internal transform of dataset by a feature transform

        Handles both csv and json data. Loads each into a dictionary to be transformed. Uses two files current and next, iterates over current and writes to next. Finally, swaps the two around ready for next transform.
        Args:
            ds (Dataset): dataset
            ft (Feature_transform): feature transform
        """
        self.active_file = open(self.next_dataset,"w")
        for j in ds:
            jNew = ft.transform(j)
            if jNew:
                jStr =  json.dumps(jNew,sort_keys=True)
                self.active_file.write(jStr+"\n")
        self.active_file.close()
        shutil.move(self.next_dataset,self.current_dataset)

    def get_dataset(self,filename):
        """get dataset 
        """
        return JsonDataSet(filename)

    def convert_to_json(self):
        ds = CsvDataSet(self.current_dataset)
        self.active_file = open(self.next_dataset,"w")
        for j in ds:
            jStr =  json.dumps(j,sort_keys=True)
            self.active_file.write(jStr+"\n")
        self.active_file.close()
        shutil.move(self.next_dataset,self.current_dataset)


    def transform(self):
        """apply all transforms in a pipeline
        """
        self.transform_init()
        self.getFeatures(self.input_folders)
        if self.data_type == 'csv':
            self.convert_to_json()
        for ft in self.pipeline:
            ds = self.get_dataset(self.current_dataset)
            self._transform(ds,ft)
        return self.get_dataset(self.current_dataset)

    def fit_transform(self):
        """fit a pipeline and then apply its transforms

        gets features, fits, transforms and stores results
        """
        self.getFeatures(self.input_folders)
        if self.data_type == 'csv':
            self.convert_to_json()
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
    """a JSON dataset
        
    Args:
        filename (str): location of JSON file
    """

    def __init__(self, filename):
        self.filename =filename
 
    def __iter__(self):
        """iterate over a JSON dataset
        """
        f = open(self.filename,"r")
        for line in f:
            line = line.rstrip()
            j = json.loads(line)
            yield j

class CsvDataSet(object):
    """a CSV Dataset
        
    Args:
        filename (str): location of JSON file
    """

    def __init__(self, filename):
        self.filename =filename
 
    def __iter__(self):
        """iterate over a CSV dataset
        """
        csvfile = open(self.filename,"r")
        reader = unicodecsv.DictReader(csvfile,encoding='utf-8')
        for d in reader:
            yield d


            
