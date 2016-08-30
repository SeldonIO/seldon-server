from seldon.pipeline.pandas_pipelines import BasePandasEstimator
from sklearn.base import BaseEstimator
import pandas as pd
from sklearn.utils import check_array
import tensorflow as tf

class TensorFlowWrapper(BasePandasEstimator,BaseEstimator):
    """
    Wrapper for tensorflow with pandas support

    Parameters
    ----------
    
    session : tensorflow session
        Contains the tensorflow model
    tf_input : tensorflow variable
        Variable used as the model input
    tf_output : tensorflow variable
        Variable used as the model output
    tf_constant : iterable
        List of tuples (tensorflow variable, value) to be used as constant for the model when doing predictions
    tmp_model : string
        url to folder where the model will be temporarily saved
    target : str
       Target column
    target_readable : str
       More descriptive version of target variable
    included : list str, optional
       columns to include
    excluded : list str, optional
       columns to exclude
    id_map : dict (int,str), optional
       map of class ids to high level names
    """
    def __init__(self,
                 session,
                 tf_input,
                 tf_output,
                 tf_constants=(),
                 tmp_model="/tmp",
                 target=None,
                 target_readable=None,
                 included=None,
                 excluded=None,
                 id_map={}):
        super(TensorFlowWrapper, self).__init__(target,target_readable,included,excluded,id_map)
        self.target = target
        self.target_readable = target_readable
        self.id_map=id_map
        self.included = included
        self.excluded = excluded
        if not self.target_readable is None:
            if self.excluded is None:
                self.excluded = [self.target_readable]
            else:
                self.excluded.append(self.target_readable)
        self.tmp_model = tmp_model
        self.sess = session
        self.tf_input = tf_input
        self.tf_output = tf_output
        self.tf_constants_vars = [('constant_%s'%i,var) for i,(var,value) in enumerate(tf_constants)]
        self.tf_constants_values = [('constant_%s'%i,value) for i,(var,value) in enumerate(tf_constants)]
        self.vectorizer = None
        
    def __getstate__(self):
        result = self.__dict__.copy()
        del result['sess']
        del result['tf_input']
        del result['tf_output']
        del result['tf_constants_vars']
        self.save(self.tmp_model)
        with open('%s/tensorflow-model.meta'%self.tmp_model, mode='rb') as metafile:
            result['_meta'] = metafile.read()
        with open('%s/tensorflow-model'%self.tmp_model, mode='rb') as modelfile:
            result['_model'] = modelfile.read()
        return result
    
    def __setstate__(self,dict):
        self.__dict__ = dict
        with open('%s/tensorflow-model.meta'%self.tmp_model, mode='wb') as metafile:
            metafile.write(dict['_meta'])
        with open('%s/tensorflow-model'%self.tmp_model, mode='wb') as modelfile:
            modelfile.write(dict['_model'])
        del self.__dict__['_meta']
        del self.__dict__['_model']
        self.load(self.tmp_model)
        
    def save(self,folder):
        """Exports tensorflow model
        """
        tf.add_to_collection('tf_input',self.tf_input)
        tf.add_to_collection('tf_output',self.tf_output)
        for var_name,var in self.tf_constants_vars:
            tf.add_to_collection(var_name,var)
        saver = tf.train.Saver()
        saver.save(self.sess,'%s/tensorflow-model'%folder)
        
    def load(self,folder):
        self.sess = tf.Session()
        new_saver = tf.train.import_meta_graph('%s/tensorflow-model.meta'%folder)
        new_saver.restore(self.sess,'%s/tensorflow-model'%folder)
        self.tf_input = self.sess.graph.get_collection('tf_input')[0]
        self.tf_output = self.sess.graph.get_collection('tf_output')[0]
        self.tf_constants_vars = []
        for var_name,value in self.tf_constants_values:
            self.tf_constants_vars.append((var_name,self.sess.graph.get_collection(var_name)[0]))

    def fit(self,X,y=None):
        pass
        
    def predict_proba(self,X):
        """
        Returns class probability estimates for the given test data.

        X : pandas dataframe or array-like
            Test samples 
        
        Returns
        -------
        proba : array-like, shape = (n_samples, n_outputs)
            Class probability estimates.
  
        """
        if isinstance(X,pd.DataFrame):
            df = X

            (X,_,_) = self.convert_numpy(df)
        else:
            check_array(X)

        all_inputs = [(self.tf_input,X)] + [(var,value) for (_,var),(_,value) in zip(self.tf_constants_vars,self.tf_constants_values)]

        return self.sess.run(self.tf_output,feed_dict={var:value for var,value in all_inputs})



