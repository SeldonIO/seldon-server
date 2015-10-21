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


numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']

def encode_onehot(df, cols, vec, op):
    """
    One-hot encoding is applied to columns specified in a pandas DataFrame.
    
    Modified from: https://gist.github.com/kljensen/5452382
    
    Details:
    
    http://en.wikipedia.org/wiki/One-hot
    http://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.OneHotEncoder.html
    
    @param df pandas DataFrame
    @param cols a list of columns to encode
    @return a DataFrame with one-hot encoding
    """

    if op == "fit":
        vec_data = pd.DataFrame(vec.fit_transform(df[cols].to_dict(outtype='records')).toarray())
    else:
        vec_data = pd.DataFrame(vec.transform(df[cols].to_dict(outtype='records')).toarray())
    vec_data.columns = vec.get_feature_names()
    vec_data.index = df.index
    
    df = df.drop(cols, axis=1)
    df = df.join(vec_data)
    return df

def default_classification_model(input_width,num_classes):
    model = Sequential()
    hidden_dimensions = int(float(input_width)*0.5)
    model.add(Dense(input_width, hidden_dimensions))
    model.add(Activation('relu'))
    model.add(Dropout(0.5))
    model.add(Dense(hidden_dimensions, num_classes))
    model.add(Activation('softmax'))
    model.compile(loss='categorical_crossentropy', optimizer='adam')
    return model


class Keras(pl.Feature_transform):

    def __init__(self,model_create=default_classification_model,target=None,batch_size=8,epochs=100,validation_split=0.1):
        self.model_create = model_create
        self.target = target
        self.vectorizer = None
        self.batch_size = batch_size
        self.epochs = epochs
        self.validation_split = validation_split

    def get_models(self):
        """get model data for this transform.
        """
        return [self.target,self.model.to_json(),self.vectorizer]
    
    def set_models(self,models):
        """set the included features
        """
        self.target = models[0]
        self.model = model_from_json(models[1])
        self.vectorizer = models[2]


    def save_model(self,folder_prefix):
        super(Keras, self).save_model(folder_prefix)
        self.model.save_weights(folder_prefix+"_weights",overwrite=True)

    def load_model(self,folder_prefix):
        super(Keras, self).load_model(folder_prefix)
        self.model.load_weights(folder_prefix+"_weights")

    def fit(self,df):
        df_y = df[self.target]
        df_keras = df.drop([self.target], axis=1)
        df_keras = df_keras.fillna(0)

        df_numeric = df_keras.select_dtypes(include=numerics)
        df_categorical = df_keras.select_dtypes(exclude=numerics)
        cat_cols = []
        if len(df_categorical.columns) > 0:
            self.vectorizer = DictVectorizer()
            for c in df_categorical:
                df_categorical = encode_onehot(df_categorical, cols=df_categorical.columns,vec=self.vectorizer,op="fit")
            df_X = pd.concat([df_numeric, df_categorical], axis=1)
        else:
            df_X = df_numeric
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
        df_keras = df_keras.fillna(0)

        df_numeric = df_keras.select_dtypes(include=numerics)
        df_categorical = df_keras.select_dtypes(exclude=numerics)
        cat_cols = []
        if len(df_categorical.columns) > 0:
            for c in df_categorical:
                df_categorical = encode_onehot(df_categorical, cols=df_categorical.columns,vec=self.vectorizer,op="fit")
            df_X = pd.concat([df_numeric, df_categorical], axis=1)
        else:
            df_X = df_numeric
        input_width = len(df_X.columns)
        num_classes = len(df_y.unique())
        test_X = df_X.as_matrix()
        return self.model.predict(test_X,verbose=1)

        
