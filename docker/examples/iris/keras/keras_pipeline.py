import sys, getopt, argparse
import seldon.pipeline.basic_transforms as bt
import seldon.pipeline.util as sutl
from sklearn.pipeline import Pipeline
import seldon.pipeline.auto_transforms as pauto
import seldon.keras as sk
from keras.models import Sequential
from keras.layers.core import Dense, Dropout, Activation
import sys


def  create_model(input_width,num_classes):
    model = Sequential()                         
    print "input width=",input_width
    model.add(Dense(5, init='uniform',input_dim=input_width))
    model.add(Activation('tanh'))
    model.add(Dropout(0.5))

    model.add(Dense(num_classes))
    model.add(Activation('softmax'))

    return model


def run_pipeline(events,models):
    tNameId = bt.Feature_id_transform(min_size=0,exclude_missing=True,zero_based=True,input_feature="name",output_feature="nameId")
    tAuto = pauto.Auto_transform(max_values_numeric_categorical=2,exclude=["nameId","name"])
    keras = sk.KerasClassifier(model_create=create_model,target="nameId",target_readable="name")
    transformers = [("tName",tNameId),("tAuto",tAuto),("keras",keras)]
    p = Pipeline(transformers)

    pw = sutl.Pipeline_wrapper()
    print events
    df = pw.create_dataframe_from_files(events)
    df2 = p.fit(df)
    pw.save_pipeline(p,models)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='keras_pipeline')
    parser.add_argument('--events', help='events folder', required=True)
    parser.add_argument('--models', help='output models folder', required=True)

    args = parser.parse_args()
    opts = vars(args)

    run_pipeline([args.events],args.models)

