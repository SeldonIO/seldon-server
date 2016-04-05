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
import logging

logger = logging.getLogger(__name__)

class Pipeline_wrapper(object):
    """
    Wrapper to allow dataframes to be created and saved from external data sources.
    Data sources:AWS s3 and file system
    Formats: JSON and CSV

    Parameters
    ----------

    work_folder : str
       load work folder to stage files
    aws_key : str, optional
       AWS key
    aws_secret : str, optional
       AWS secret
    """
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
            logger.info("creating %s",self.work_folder)
            os.makedirs(self.work_folder)


    #
    # save dataframe to location
    #

    def save_dataframe(self,df,location,df_format="json",csv_index=True):
        """Save dataframe

        Parameters
        ----------

        df : pandas daraframe
           dataframe to save
        location : str
           external filesystem location to save to
        df_format : str
           format to use : json or csv
        csv_index : bool
           whether to save index when outputing to csv
        """
        self.create_work_folder()
        tmp_file = self.work_folder+"/df_tmp"
        if df_format == 'csv':
            logger.info("saving dataframe as csv")
            df.to_csv(tmp_file,index=csv_index)
        else:
            logger.info("saving dataframe as json")
            f = open(tmp_file,"w")
            for i in range(0, df.shape[0]):
                row = df.irow(i).dropna()
                jNew = row.to_dict()
                jStr =  json.dumps(jNew,sort_keys=True)
                f.write(jStr+"\n")
            f.close()
        futil = fu.FileUtil(aws_key=self.aws_key,aws_secret=self.aws_secret)
        futil.copy(tmp_file,location)

    #
    # download data and convert to dataframe
    #
    

    def _save_features_local(self,line):
        """save data line to local features file
        """
        if not self.df_format == 'csv' and self.lines_read > 0:
            self.active_file.write(",")
        self.active_file.write(line+"\n")
        self.lines_read += 1


    def _copy_features_locally(self,locations,local_file,df_format):
        self.df_format=df_format
        self.create_work_folder()
        logger.info("streaming features %s to %s",locations,local_file)
        logger.info("input type is %s",self.df_format)
        self.lines_read = 0
        self.active_file = open(local_file,"w")
        if not self.df_format == 'csv':
            self.active_file.write("[")
        futil = fu.FileUtil(aws_key=self.aws_key,aws_secret=self.aws_secret)
        futil.stream_multi(locations,self._save_features_local)
        if not self.df_format == 'csv':
            self.active_file.write("]")
        self.active_file.close()
        logger.info("finished stream of features")

    def _convert_dataframe(self,local_file,df_format,csv_dates=None,index_col=None):
        """load csv or json into pandas dataframe
        """
        logger.info("loading data into pandas dataframe")
        if df_format == 'csv':
            logger.info("loading csv %s index:%s",csv_dates,index_col)
            return pd.read_csv(local_file,parse_dates=csv_dates,index_col=index_col)
        else:
            logger.info("loading json")
            return pd.read_json(local_file,orient='records')


    def create_dataframe(self,data=None,df_format="json",csv_dates=None,index_col=None):
        """
        Create Pandas dataframe from external source

        Parameters
        ----------

        data : object, list, dict or str
           object : pandas dataframe - will be returned as is
           list : list of folders to load data frame
           str : filename to load data frome
           dict : data in dict
        """
        if data is not None:
            if isinstance(data, pd.DataFrame):
                return data
            elif isinstance(data,dict):
                return pd.DataFrame([data])
            elif isinstance(data,basestring):
                local_file= self.work_folder+"/data"
                futil = fu.FileUtil(aws_key=self.aws_key,aws_secret=self.aws_secret)
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
        """
        Save scikit learn pipeline to external location

        Parameters
        ----------

        pipelines : sklearn pipeline
           pipeline to be saved
        location : str
           external folder to save pipeline
        """
        self.create_work_folder()
        pipeline_folder = self.work_folder+"/pipeline"
        if not os.path.exists(pipeline_folder):
            logger.info("creating folder %s",pipeline_folder)
            os.makedirs(pipeline_folder)
        tmp_file = pipeline_folder+"/p"
        joblib.dump(pipeline,tmp_file)
        futil = fu.FileUtil(aws_key=self.aws_key,aws_secret=self.aws_secret)
        futil.copy(pipeline_folder,location)


    def load_pipeline(self,pipeline_folder):
        """
        Load scikit learn pipeline from external folder
        
        Parameters
        ----------

        pipeline_folder : str
           external folder holding pipeline
        """
        self.create_work_folder()
        local_pipeline_folder = self.work_folder+"/pipeline"
        if not os.path.exists(local_pipeline_folder):
            logger.info("creating folder %s",local_pipeline_folder)
            os.makedirs(local_pipeline_folder)
        futil = fu.FileUtil(aws_key=self.aws_key,aws_secret=self.aws_secret)
        futil.copy(pipeline_folder,local_pipeline_folder)
        return joblib.load(local_pipeline_folder+"/p")




            
