import sys, getopt, argparse
import seldon.pipeline.basic_transforms as bt
import seldon.pipeline.auto_transforms as pauto
import seldon.pipeline.util as sutl
from sklearn.pipeline import Pipeline
import seldon.vw as vw
import sys

def run_pipeline(events,models):
    tNameId = bt.Feature_id_transform(min_size=0,exclude_missing=True,zero_based=True,input_feature="name",output_feature="nameId")
    tAuto = pauto.Auto_transform(max_values_numeric_categorical=2,exclude=["nameId","name"])
    vwc = vw.VWClassifier(target="nameId",target_readable="name")
    transformers = [("tName",tNameId),("tAuto",tAuto),("vw",vwc)]
    p = Pipeline(transformers)

    pw = sutl.Pipeline_wrapper()
    df = pw.create_dataframe_from_files(events)
    df2 = p.fit(df)
    pw.save_pipeline(p,models)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='xgb_pipeline')
    parser.add_argument('--events', help='events folder', required=True)
    parser.add_argument('--models', help='output models folder', required=True)

    args = parser.parse_args()
    opts = vars(args)

    run_pipeline([args.events],args.models)

