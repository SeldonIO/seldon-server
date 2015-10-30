import sys, getopt, argparse
import seldon.pipeline.basic_transforms as bt
import seldon.pipeline.pipelines as pl
import seldon.pipeline.auto_transforms as pauto
import seldon.vw as vw
import sys

def run_pipeline(events,models):
    p = pl.Pipeline(input_folders=events,local_models_folder="models_tmp")
    tNameId = bt.Feature_id_transform(min_size=0,exclude_missing=True,zero_based=False)
    tNameId.set_input_feature("name")
    tNameId.set_output_feature("nameId")
    tAuto = pauto.Auto_transform(max_values_numeric_categorical=2,exclude=["nameId","name"])
    vwc = vw.VWClassifier(target="nameId",target_readable="name",excluded=["name"])
    p.add(tNameId)
    p.add(tAuto)
    p.add(vwc)
    df = p.fit_transform()
    p.save_models(models)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='bbm_pipeline')
    parser.add_argument('--events', help='events folder', required=True)
    parser.add_argument('--models', help='output models folder', required=True)

    args = parser.parse_args()
    opts = vars(args)

    run_pipeline([args.events],args.models)

