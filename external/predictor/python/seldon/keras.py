from __future__ import absolute_import
from keras.models import Sequential
from keras.layers.core import Dense, Dropout, Activation
from keras.layers.normalization import BatchNormalization
from keras.utils import np_utils
from keras.models import model_from_json
import sys
import seldon.pipeline.pipelines as pl
import pandas as pd
from sklearn.feature_extraction import DictVectorizer
import json


def default_classification_model(input_width,num_classes):
    model = Sequential()
    hidden_dimensions = int(float(input_width)*0.75)
    model.add(Dense(input_width, hidden_dimensions))
    model.add(Activation('relu'))
    model.add(Dropout(0.5))
    model.add(Dense(hidden_dimensions, num_classes))
    model.add(Activation('softmax'))
    model.compile(loss='categorical_crossentropy', optimizer='adam')
    return model


class Keras(pl.Estimator,pl.Feature_transform):

    def __init__(self,model_create=default_classification_model,target=None, target_readable=None,included=None,excluded=None,batch_size=8,epochs=100,validation_split=0.1):
        self.model_create = model_create
        self.target = target
        self.target_readable = target_readable
        self.set_class_id_map({})
        self.included = included
        self.excluded = excluded
        self.vectorizer = None
        self.batch_size = batch_size
        self.epochs = epochs
        self.validation_split = validation_split
        self.model_suffix = "_keras_model"
        self.weights_suffix = "_keras_weights"

    def get_models(self):
        """get model data for this transform.
        """
        return [self.target,self.included,self.excluded,self.model.to_json(),self.vectorizer,self.get_class_id_map()]
    
    def set_models(self,models):
        """set the included features
        """
        self.target = models[0]
        self.included = models[1]
        self.excluded = models[2]
        self.model = model_from_json(models[3])
        self.vectorizer = models[4]
        self.set_class_id_map(models[5])


    def save_model(self,folder_prefix):
        super(Keras, self).save_model(folder_prefix+self.model_suffix)
        self.model.save_weights(folder_prefix+self.weights_suffix,overwrite=True)

    def load_model(self,folder_prefix):
        super(Keras, self).load_model(folder_prefix+self.model_suffix)
        self.model.load_weights(folder_prefix+self.weights_suffix)

    def _exclude_include_features(self,df_keras):
        if not self.included is None:
            df_keras = df_keras(self.included)
        elif not self.excluded is None:
            df_keras = df_keras.drop(set(self.excluded).intersection(df_keras.columns), axis=1)
        return df_keras

    def fit(self,df):
        df_y = df[self.target]
        if not self.target_readable is None:
            self.create_class_id_map(df,self.target,self.target_readable)
        df_keras = df.drop([self.target], axis=1)
        df_keras = self._exclude_include_features(df_keras)
        df_keras = df_keras.fillna(0)

        (df_X,self.vectorizer) = self.convert_dataframe(df_keras,self.vectorizer)
        input_width = len(df_X.columns)
        num_classes = len(df_y.unique())
        train_X = df_X.as_matrix()
        train_y = np_utils.to_categorical(df_y, num_classes)
        self.model = self.model_create(input_width,num_classes)
        history = self.model.fit(train_X, train_y, nb_epoch=self.epochs, batch_size=self.batch_size, verbose=1, show_accuracy=True, validation_split=self.validation_split)
        return df

    def predict_proba(self,df):
        if self.target in df:
            df_y = df[self.target]
            df_keras = df.drop([self.target], axis=1)
        else:
            df_y = None
            df_keras = df
        df_keras = self._exclude_include_features(df_keras)
        df_keras = df_keras.fillna(0)

        (df_X,_) = self.convert_dataframe(df_keras,self.vectorizer)
        test_X = df_X.as_matrix()
        return self.model.predict(test_X,verbose=1)

        
