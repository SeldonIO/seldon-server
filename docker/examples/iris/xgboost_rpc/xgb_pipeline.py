import sys, getopt, argparse
import seldon.pipeline.basic_transforms as bt
import seldon.pipeline.util as sutl
import seldon.pipeline.auto_transforms as pauto
from sklearn.pipeline import Pipeline
import seldon.xgb as xg
import seldon.pipeline.cross_validation as cf
import sys
import logging

def run_pipeline(events,models):

    tNameId = bt.Feature_id_transform(min_size=0,exclude_missing=True,zero_based=True,input_feature="name",output_feature="nameId")
    tAuto = pauto.Auto_transform(max_values_numeric_categorical=2,exclude=["nameId","name"])
    xgb = xg.XGBoostClassifier(target="nameId",target_readable="name",excluded=["name"],learning_rate=0.1,silent=1)
    cv = cf.Seldon_KFold(xgb,5)
    logger.info("cross validation scores %s",cv.get_scores())

    transformers = [("tName",tNameId),("tAuto",tAuto),("cv",cv)]
    p = Pipeline(transformers)

    pw = sutl.Pipeline_wrapper()
    df = pw.create_dataframe(events)
    df2 = p.fit_transform(df)
    pw.save_pipeline(p,models)
    logger.info("cross validation scores %s",cv.get_scores())


if __name__ == '__main__':
    logger = logging.getLogger()
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(name)s : %(message)s', level=logging.DEBUG)
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser(prog='xgb_pipeline')
    parser.add_argument('--events', help='events folder', required=True)
    parser.add_argument('--models', help='output models folder', required=True)

    args = parser.parse_args()
    opts = vars(args)

    run_pipeline([args.events],args.models)

