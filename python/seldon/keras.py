from __future__ import absolute_import
from keras.models import Sequential
from keras.layers.core import Dense, Dropout, Activation
from keras.layers.normalization import BatchNormalization
from keras.utils import np_utils
from keras.models import model_from_json
import sys
import numpy as np
import pandas as pd
from sklearn.feature_extraction import DictVectorizer
from sklearn.utils import check_X_y
from sklearn.utils import check_array
from seldon.pipeline.pandas_pipelines import BasePandasEstimator 
from sklearn.base import BaseEstimator,ClassifierMixin
from keras.utils.np_utils import to_categorical
from keras.models import model_from_json
import copy
import logging

logger = logging.getLogger(__name__)

def default_classification_model(input_width,num_classes):
    """Default classification model
    """
    model = Sequential()                         
    logger.info("input width=%d",input_width)
    model.add(Dense(5, init='uniform',input_dim=input_width))
    model.add(Activation('tanh'))

    model.add(Dense(num_classes))
    model.add(Activation('softmax'))

    return model


class KerasClassifier(BasePandasEstimator,BaseEstimator,ClassifierMixin):
    def __init__(self,model_create=default_classification_model,tmp_model="/tmp/model",target=None, target_readable=None,included=None,excluded=None,id_map={},optimizer='adam', loss='categorical_crossentropy', train_batch_size=128, test_batch_size=128, nb_epoch=100, shuffle=True, show_accuracy=False, validation_split=0, validation_data=None, callbacks=None,verbose=0):
        """
        Wrapper for keras with pandas support
        Derived from https://github.com/fchollet/keras/blob/master/keras/wrappers/scikit_learn.py
    
        Parameters
        ----------
           
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
        optimizer : str, optional
           Optimizer to use in training
        loss : str, optional
           loss to appy
        train_batch_size : int, optional
           Number of training samples evaluated at a time.
        test_batch_size : int, optional
           Number of test samples evaluated at a time.
        nb_epochs : int, optional
           Number of training epochs.
        shuffle : boolean, optional
           Wheter to shuffle the samples at each epoch.
        show_accuracy : boolean, optional
           Whether to display class accuracy in the logs at each epoch.
        validation_split : float [0, 1], optional
           Fraction of the data to use as held-out validation data.
        validation_data : tuple (X, y), optional
           Data to be used as held-out validation data. Will override validation_split.
        callbacks : list, optional
           List of callbacks to apply during training.
        verbose : int, optional
           Verbosity level.
    """
        super(KerasClassifier, self).__init__(target,target_readable,included,excluded,id_map)
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
        self.vectorizer=None
        self.model_create=model_create
        self.optimizer=optimizer 
        self.loss=loss 
        self.train_batch_size=train_batch_size 
        self.test_batch_size=test_batch_size 
        self.nb_epoch=nb_epoch
        self.shuffle=shuffle
        self.show_accuracy=show_accuracy
        self.validation_split=validation_split
        self.validation_data=validation_data
        self.callbacks = [] if callbacks is None else callbacks
        self.verbose=verbose
        self.tmp_model=tmp_model
        self.compiled_model_ = None

    def __getstate__(self):
        """Remove parts of class that cause issue in pickling. Can recreate them in setstate
        """
        result = self.__dict__.copy()
        del result['compiled_model_']
        del result['model_create']
        return result

    def __setstate__(self, dict):
        """Create compiled model from parameters. saving model to file and loading it back in
        """
        self.__dict__ = dict
        self.model_create=None
        with open(self.tmp_model, mode='wb') as modelfile: # b is important -> binary
            modelfile.write(self.model_saved)
        self.compiled_model_ = model_from_json(self.config_)
        self.compiled_model_.load_weights(self.tmp_model)
        self.compiled_model_.compile(optimizer=self.optimizer, loss=self.loss)

    def fit(self,X,y=None):
        """Derived from https://github.com/fchollet/keras/blob/master/keras/wrappers/scikit_learn.py
        Adds:
        Handling pandas inputs
        Saving of model into the class to allow for easy pickling

        Parameters
        ----------

        X : pandas dataframe or array-like
           training samples
        y : array like, required for array-like X and not used presently for pandas dataframe
           class labels

        Returns
        -------
        self: object

        """
        if isinstance(X,pd.DataFrame):
            df = X
            (X,y,self.vectorizer) = self.convert_numpy(df)
        else:
            check_X_y(X,y)

        input_width = X.shape[1]
        num_classes = len(y.unique())
        logger.info("input_width %d",input_width)
        logger.info("num_classes %d",num_classes)
        train_y = np_utils.to_categorical(y, num_classes)
        self.model = self.model_create(input_width,num_classes)

        if len(y.shape) == 1:
            self.classes_ = list(np.unique(y))
            if self.loss == 'categorical_crossentropy':
                y = to_categorical(y)
        else:
            self.classes_ = np.arange(0, y.shape[1])
        
        if self.compiled_model_ is None:
            self.compiled_model_ = copy.deepcopy(self.model)
            self.compiled_model_.compile(optimizer=self.optimizer, loss=self.loss)
        history = self.compiled_model_.fit(
            X, y, batch_size=self.train_batch_size, nb_epoch=self.nb_epoch, verbose=self.verbose,
            shuffle=self.shuffle, show_accuracy=self.show_accuracy,
            validation_split=self.validation_split, validation_data=self.validation_data,
            callbacks=self.callbacks)

        self.config_ = self.model.to_json()
        self.compiled_model_.save_weights(self.tmp_model)
        with open(self.tmp_model, mode='rb') as file: # b is important -> binary
            self.model_saved = file.read()
        return self

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
        return self.compiled_model_.predict_proba(X, batch_size=self.test_batch_size, verbose=self.verbose)

