import seldon.fileutil as fu
import json
from sklearn.externals import joblib
from sklearn.feature_extraction import DictVectorizer
import os.path
import logging
import shutil 
import unicodecsv
import pandas as pd


class Estimator(object):

    def __init__(self):
        print "Estimator init called"
        self.id_map = None

    def predict_proba(self,df=None):
        raise NotImplementedError("predict_proba not implemented")

    def set_class_id_map(self,id_map):
        self.id_map = id_map

    def get_class_id_map(self):
        return self.id_map

    def create_class_id_map(self,df,target,target_readable):
        ids = df.drop_duplicates([target,target_readable]).to_dict(orient='records')
        m = {}
        for d in ids:
            m[d[target]] = d[target_readable]
        print "id map ",m
        self.set_class_id_map(m)

    def encode_onehot(self,df, cols, vec, op):
        if op == "fit":
            vec_data = pd.DataFrame(vec.fit_transform(df[cols].to_dict(outtype='records')).toarray())
        else:
            vec_data = pd.DataFrame(vec.transform(df[cols].to_dict(outtype='records')).toarray())
        vec_data.columns = vec.get_feature_names()
        vec_data.index = df.index
        
        df = df.drop(cols, axis=1)
        df = df.join(vec_data)
        return df

    def convert_dataframe(self,df_base,vectorizer):
        if vectorizer is None:
            vectorizer = DictVectorizer()
            op = "fit"
        else:
            op = "transform"
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        df_numeric = df_base.select_dtypes(include=numerics)
        df_categorical = df_base.select_dtypes(exclude=numerics)
        cat_cols = []
        if len(df_categorical.columns) > 0:
            df_categorical = encode_onehot(df_categorical, cols=df_categorical.columns,vec=vectorizer,op=op)
            df_X = pd.concat([df_numeric, df_categorical], axis=1)
        else:
            df_X = df_numeric
        return (df_X,vectorizer)

    def close(self):
        pass



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

    def save_model(self,folder_prefix):
        models = self.get_models()
        joblib.dump(models,folder_prefix)

    def load_model(self,folder_prefix):
        models = joblib.load(folder_prefix)
        self.set_models(models)

    def fit(self,df):
        """fit method by default does nothing
        """
        pass

    def transform(self,df):
        """transform by default does nothing
        """
        return df

    def set_logging(self,logger):
        """setter for logging 
        """
        self.logger = logger

    def get_log_prefix(self):
        """get a useful description for logging
        """
        return self.__class__.__module__ + "." + self.__class__.__name__+" "+self.input_feature+"->"+self.output_feature

    def close(self):
        pass

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

        output_format [Optional(str)]: output file format

        csv_dates [Optional(list str)]: optional csv columns to parse as dates when loading initial data
    """

    def __init__(self,input_folders=[],output_folder=None,models_folder=None,local_models_folder="./models",local_data_folder="./data",aws_key=None,aws_secret=None,data_type='json',output_format=None,csv_dates=False,index_col=None):
        self.pipeline = []
        self.input_folders = input_folders
        self.output_folder = output_folder
        self.models_folder = models_folder
        self.local_models_folder = local_models_folder
        self.local_data_folder = local_data_folder
        self.fu = fu.FileUtil(key=aws_key,secret=aws_secret)
        self.logger = logging.getLogger('seldon')
        self.logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.current_dataset = self.local_data_folder + "/current"
        self.data_type = data_type
        self.output_format = output_format
        self.csv_dates = csv_dates
        self.index_col = index_col
        self.loaded_models = False

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

    def _save_pipeline(self):
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
        self.pipeline = []
        f =open(self.local_models_folder+"/pipeline.txt")
        for line in f:
            line = line.rstrip()
            cl = self.get_class(line)
            t = cl()
            self.add(t)
        f.close()

    def save_models(self,folder):
        print "saving models to ",folder
        self._store_models()
        self._save_pipeline()
        self._upload_models(folder)


    def _upload_models(self,models_folder):
        """upload models from local folder to final models folder
        """
        self.fu.copy(self.local_models_folder,models_folder)

    def download_models(self):
        """download models to local folder
        """
        if self.models_folder:
            if not os.path.exists(self.local_models_folder):
                os.makedirs(self.local_models_folder)
            self.fu.copy(self.models_folder,self.local_models_folder)

    def save_features(self,df,location):
        self._save_dataframe(df)
        self._store_features(location)

    def _save_dataframe(self,df):
        """save dataframe to csv or json
        """
        out_type = self.data_type
        if self.output_format:
            out_type = self.output_format
        if out_type == 'csv':
            print "saving dataframe as csv"
            df.to_csv(self.current_dataset,index=True)
        else:
            print "saving dataframe as json"
            f = open(self.current_dataset,"w")
            for i in range(0, df.shape[0]):
                row = df.irow(i).dropna()
                jNew = row.to_dict()
                jStr =  json.dumps(jNew,sort_keys=True)
                f.write(jStr+"\n")
            f.close()


    def _store_features(self,output_folder):
        """store features in final location folder
        """
        self.fu.copy(self.current_dataset,output_folder+"/features")

    def _store_models(self):
        """for each transformation in pipeline store model to local folder
        """
        if not os.path.exists(self.local_models_folder):
            os.makedirs(self.local_models_folder)
        pos = 1
        for t in self.pipeline:
            models = t.get_models()
            t.save_model(self.local_models_folder+"/"+str(pos))
            pos += 1

    def load_models(self):
        """load models for each transformation in pipeline from local folder
        """
        pos = 1
        for t in self.pipeline:
            t.load_model(self.local_models_folder+"/"+str(pos))
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
        if not self.data_type == 'csv' and self.lines_read > 0:
            self.active_file.write(",")
        self.active_file.write(line+"\n")
        self.lines_read += 1


    def copy_features_locally(self,locations):
        """get features from folders and store locally the data

        Args:
            locations (list): list of folders
        """
        print "streaming features ",locations," to ",self.current_dataset
        print "input type is ",self.data_type
        self.lines_read = 0
        self.active_file = open(self.current_dataset,"w")
        if not self.data_type == 'csv':
            self.active_file.write("[")
        self.fu.stream_multi(locations,self.save_features_local)
        if not self.data_type == 'csv':
            self.active_file.write("]")
        self.active_file.close()
        print "finished stream of features"

    def convert_dataframe(self):
        """load csv or json into pandas dataframe
        """
        print "loading data into pandas dataframe"
        if self.data_type == 'csv':
            print "loading csv ",self.csv_dates,"index ",self.index_col
            return pd.read_csv(self.current_dataset,parse_dates=self.csv_dates,index_col=self.index_col)
        else:
            print "loading json"
            return pd.read_json(self.current_dataset,orient='records')


    def init_models(self):
        """prepare for transform

        download models locally, load the pipeline and set its models
        """
        if not self.loaded_models:
            self.download_models()
            self.load_pipeline()
            self.load_models()

    def create_dataframe(self,data=None):
        if data is not None:
            if isinstance(data, pd.DataFrame):
                return data
            elif isinstance(data,dict):
                df = pd.DataFrame([data])
            else:
                raise ValueError("unknown argument type for data")
        else:
            self.copy_features_locally(self.input_folders)
            df = self.convert_dataframe()
        return df

    def transform(self,data=None):
        """apply all transforms in a pipeline
        """
        self.init_models()
        df = self.create_dataframe(data)
        for ft in self.pipeline:
            df = ft.transform(df)
        return df

    def predict_proba(self,data=None):
        """apply all transforms except last in a pipeline and then call predict_proba on last
        """
        self.init_models()
        df = self.create_dataframe(data)
        for ft in self.pipeline[:-1]:
            df = ft.transform(df)
        return self.pipeline[-1].predict_proba(df)

    def get_estimator_class_ids(self):
        return self.pipeline[-1].get_class_id_map()

    def fit(self,data=None):
        """fit a pipeline

        gets features, fits, and stores models
        """
        df = self.create_dataframe(data)
        for ft in self.pipeline:
            ft.fit(df)
        if not self.models_folder is None:
            self.save_models(self.models_folder)

    def fit_transform(self,data=None):
        """fit a pipeline and then apply its transforms

        gets features, fits, transforms and stores results
        """
        df = self.create_dataframe(data)
        for ft in self.pipeline:
            ft.fit(df)
            df = ft.transform(df)
        if not self.models_folder is None:
            self.save_models(self.models_folder)
        if not self.output_folder is None:
            self.save_features(df,self.output_folder)
        return df

    def close(self):
        """ clear and release any resources held by parts of the pipeline
        """
        for ft in self.pipeline:
            ft.close()
        


            
