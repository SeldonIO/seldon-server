import sys, getopt, argparse
import seldon.pipeline.basic_transforms as bt
import seldon.pipeline.pipelines as pl
import seldon.pipeline.auto_transforms as pauto
import seldon.keras as sk
from keras.models import Sequential
from keras.layers.core import Dense, Dropout, Activation
import sys


def  create_model(input_width,num_classes):
    model = Sequential()                         
    print "input width=",input_width
    model.add(Dense(5, init='uniform',input_dim=4))
    model.add(Activation('tanh'))
    model.add(Dropout(0.5))

    model.add(Dense(num_classes))
    model.add(Activation('softmax'))
    model.compile(loss='categorical_crossentropy', optimizer='adam')
    return model


def run_pipeline(events,models):
    p = pl.Pipeline(input_folders=events,local_models_folder="models_tmp")
    tNameId = bt.Feature_id_transform(min_size=0,exclude_missing=True,zero_based=True)
    tNameId.set_input_feature("name")
    tNameId.set_output_feature("nameId")
    tAuto = pauto.Auto_transform(max_values_numeric_categorical=2,exclude=["nameId","name"])
    keras = sk.Keras(model_create=create_model,target="nameId",target_readable="name",excluded=["name"],batch_size=16,epochs=300)
    p.add(tNameId)
    p.add(tAuto)
    p.add(keras)
    df = p.fit_transform()
    p.save_models(models)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='bbm_pipeline')
    parser.add_argument('--events', help='events folder', required=True)
    parser.add_argument('--models', help='output models folder', required=True)

    args = parser.parse_args()
    opts = vars(args)

    run_pipeline([args.events],args.models)

