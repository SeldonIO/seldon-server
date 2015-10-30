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


def default_classification_model(input_width,num_classes):
    model = Sequential()                         
    print "input width=",input_width
    model.add(Dense(5, init='uniform',input_dim=4))
    model.add(Activation('tanh'))
    model.add(Dropout(0.5))

    model.add(Dense(num_classes))
    model.add(Activation('softmax'))
    model.compile(loss='categorical_crossentropy', optimizer='adam')
    return model

class Keras(pl.Estimator,pl.Feature_transform):
    def __init__(self,model_create=default_classification_model,target=None, target_readable=None,included=None,excluded=None,batch_size=8,epochs=100,validation_split=0.1):
        super(Keras, self).__init__(target,target_readable,included,excluded)
        self.model_create = model_create
        self.batch_size = batch_size
        self.epochs = epochs
        self.validation_split = validation_split
        self.model_suffix = "_keras_model"
        self.weights_suffix = "_keras_weights"

    def get_models(self):
        """get model data for this transform.
        """
        return super(Keras, self).get_models_estimator() + [self.model.to_json()]
    
    def set_models(self,models):
        """set the included features
        """
        models = super(Keras, self).set_models_estimator(models)
        self.model = model_from_json(models[0])

    def save_model(self,folder_prefix):
        super(Keras, self).save_model(folder_prefix+self.model_suffix)
        self.model.save_weights(folder_prefix+self.weights_suffix,overwrite=True)

    def load_model(self,folder_prefix):
        super(Keras, self).load_model(folder_prefix+self.model_suffix)
        self.model.load_weights(folder_prefix+self.weights_suffix)


    def fit(self,df):
        (X,y,self.vectorizer) = self.convert_numpy(df)
        input_width = X.shape[1]
        num_classes = len(y.unique())
        train_y = np_utils.to_categorical(y, num_classes)
        self.model = self.model_create(input_width,num_classes)
        history = self.model.fit(X, train_y, nb_epoch=self.epochs, batch_size=self.batch_size, verbose=1, show_accuracy=True, validation_split=self.validation_split)
        return df

    def predict_proba(self,df):
        (X,_,_) = self.convert_numpy(df)
        return self.model.predict(X,verbose=1)

        
