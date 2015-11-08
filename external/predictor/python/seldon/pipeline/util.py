import seldon.fileutil as fu
import json
import os.path
import logging
import shutil 
import unicodecsv
import numpy as np
import pandas as pd
import random
import string
from sklearn.externals import joblib

class Pipeline_wrapper(object):
    
    def __init__(self,work_folder="/tmp",aws_key=None,aws_secret=None):
        self.work_folder=work_folder
        self.lines_read = 0
        self.df_format='json'
        self.active_file=None
        self.aws_key=aws_key
        self.aws_secret=aws_secret

    def get_work_folder(self):
        return self.work_folder

    def create_work_folder(self):
        if not os.path.exists(self.work_folder):
            print "creating ",self.work_folder
            os.makedirs(self.work_folder)


    #
    # save dataframe to location
    #

    def save_dataframe(self,df,location,df_format="json",csv_index=True):
        """Save dataframe
        """
        """save dataframe to csv or json
        """
        self.create_work_folder()
        tmp_file = self.work_folder+"/df_tmp"
        if df_format == 'csv':
            print "saving dataframe as csv"
            df.to_csv(tmp_file,index=csv_index)
        else:
            print "saving dataframe as json"
            f = open(tmp_file,"w")
            for i in range(0, df.shape[0]):
                row = df.irow(i).dropna()
                jNew = row.to_dict()
                jStr =  json.dumps(jNew,sort_keys=True)
                f.write(jStr+"\n")
            f.close()
        futil = fu.FileUtil(key=self.aws_key,secret=self.aws_secret)
        futil.copy(tmp_file,location)

    #
    # download data and convert to dataframe
    #
    

    def _save_features_local(self,line):
        """save data line to local features file

        Args:
            line (str): features data line
        """
        if not self.df_format == 'csv' and self.lines_read > 0:
            self.active_file.write(",")
        self.active_file.write(line+"\n")
        self.lines_read += 1


    def _copy_features_locally(self,locations,local_file,df_format):
        self.df_format=df_format
        self.create_work_folder()
        print "streaming features ",locations," to ",local_file
        print "input type is ",self.df_format
        self.lines_read = 0
        self.active_file = open(local_file,"w")
        if not self.df_format == 'csv':
            self.active_file.write("[")
        futil = fu.FileUtil(key=self.aws_key,secret=self.aws_secret)
        futil.stream_multi(locations,self._save_features_local)
        if not self.df_format == 'csv':
            self.active_file.write("]")
        self.active_file.close()
        print "finished stream of features"

    def _convert_dataframe(self,local_file,df_format,csv_dates=None,index_col=None):
        """load csv or json into pandas dataframe
        """
        print "loading data into pandas dataframe"
        if df_format == 'csv':
            print "loading csv ",csv_dates,"index ",index_col
            return pd.read_csv(local_file,parse_dates=csv_dates,index_col=index_col)
        else:
            print "loading json"
            return pd.read_json(local_file,orient='records')


    def create_dataframe(self,data=None,df_format="json",csv_dates=None,index_col=None):
        if data is not None:
            if isinstance(data, pd.DataFrame):
                return data
            elif isinstance(data,dict):
                return pd.DataFrame([data])
            elif isinstance(data,basestring):
                local_file= self.work_folder+"/data"
                futil = fu.FileUtil(key=self.aws_key,secret=self.aws_secret)
                futil.copy(data,local_file)
                return self._convert_dataframe(local_file,df_format,csv_dates,index_col)
            elif isinstance(data,list):
                local_file= self.work_folder+"/data"
                self._copy_features_locally(data,local_file,df_format)
                return self._convert_dataframe(local_file,df_format,csv_dates,index_col)
            else:
                raise ValueError("unknown argument type for data")
        


    #
    # Upload pipeline
    #

    def save_pipeline(self,pipeline,location):
        self.create_work_folder()
        pipeline_folder = self.work_folder+"/pipeline"
        if not os.path.exists(pipeline_folder):
            print "creating folder ",pipeline_folder
            os.makedirs(pipeline_folder)
        tmp_file = pipeline_folder+"/p"
        joblib.dump(pipeline,tmp_file)
        futil = fu.FileUtil(key=self.aws_key,secret=self.aws_secret)
        futil.copy(pipeline_folder,location)


    def load_pipeline(self,pipeline_folder):
        self.create_work_folder()
        local_pipeline_folder = self.work_folder+"/pipeline"
        if not os.path.exists(local_pipeline_folder):
            print "creating folder ",local_pipeline_folder
            os.makedirs(local_pipeline_folder)
        futil = fu.FileUtil(key=self.aws_key,secret=self.aws_secret)
        futil.copy(pipeline_folder,local_pipeline_folder)
        return joblib.load(local_pipeline_folder+"/p")


    def get_estimator_class_ids(self):
        return self.pipeline[-1].get_class_id_map()

        


            
