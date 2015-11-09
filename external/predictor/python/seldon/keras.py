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
from seldon.pipeline.pandas_pipelines import PandasEstimator 
from sklearn.base import BaseEstimator,ClassifierMixin
from keras.utils.np_utils import to_categorical
from keras.models import model_from_json
import copy

def default_classification_model(input_width,num_classes):
    """Default classification model
    """
    model = Sequential()                         
    print "input width=",input_width
    model.add(Dense(5, init='uniform',input_dim=input_width))
    model.add(Activation('tanh'))
    model.add(Dropout(0.5))

    model.add(Dense(num_classes))
    model.add(Activation('softmax'))

    return model


class KerasClassifier(PandasEstimator,BaseEstimator,ClassifierMixin):
    def __init__(self,model_create=default_classification_model,tmp_model="/tmp/model",target=None, target_readable=None,included=None,excluded=None,id_map={},optimizer='adam', loss='categorical_crossentropy', train_batch_size=128, test_batch_size=128, nb_epoch=100, shuffle=True, show_accuracy=False, validation_split=0, validation_data=None, callbacks=None,verbose=0):
        """
            Derived from https://github.com/fchollet/keras/blob/master/keras/wrappers/scikit_learn.py
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

    def __getstate__(self):
        result = self.__dict__.copy()
        del result['compiled_model_']
        del result['model_create']
        return result

    def __setstate__(self, dict):
        self.__dict__ = dict
        self.model_create=None
        with open(self.tmp_model, mode='wb') as modelfile: # b is important -> binary
            modelfile.write(self.model_saved)
        self.compiled_model_ = model_from_json(self.config_)
        self.compiled_model_.load_weights(self.tmp_model)
        self.compiled_model_.compile(optimizer=self.optimizer, loss=self.loss)

    def fit(self,X,y=None):
        if isinstance(X,pd.DataFrame):
            df = X
            (X,y,self.vectorizer) = self.convert_numpy(df)
        else:
            check_X_y(X,y)

        input_width = X.shape[1]
        num_classes = len(y.unique())
        print "input_width",input_width
        print "num_classes",num_classes
        train_y = np_utils.to_categorical(y, num_classes)
        self.model = self.model_create(input_width,num_classes)

        if len(y.shape) == 1:
            self.classes_ = list(np.unique(y))
            if self.loss == 'categorical_crossentropy':
                y = to_categorical(y)
        else:
            self.classes_ = np.arange(0, y.shape[1])

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
        if isinstance(X,pd.DataFrame):
            df = X
            (X,_,_) = self.convert_numpy(df)
        else:
            check_array(X)
        return self.compiled_model_.predict_proba(X, batch_size=self.test_batch_size, verbose=self.verbose)

