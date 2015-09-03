import sys, getopt, argparse
import seldon.pipeline.basic_transforms as bt
import seldon.pipeline.pipelines as pl
import sys

def run_pipeline(events,features,models):
    p = pl.Pipeline(input_folders=events,output_folder=features,local_models_folder="models_tmp",models_folder=models)

    tNameId = bt.Feature_id_transform(min_size=0,exclude_missing=True)
    tNameId.set_input_feature("name")
    tNameId.set_output_feature("nameId")
    svmTransform = bt.Svmlight_transform(included=["f1","f2","f3","f4"] )
    svmTransform.set_output_feature("svmfeatures")
    p.add(tNameId)
    p.add(svmTransform)
    p.fit_transform()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='bbm_pipeline')
    parser.add_argument('--events', help='events folder', required=True)
    parser.add_argument('--features', help='output features folder', required=True)
    parser.add_argument('--models', help='output models folder', required=True)

    args = parser.parse_args()
    opts = vars(args)

    run_pipeline([args.events],args.features,args.models)

