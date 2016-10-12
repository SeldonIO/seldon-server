import sys, getopt, argparse
import seldon.pipeline.basic_transforms as bt
import seldon.pipeline.util as sutl
import seldon.pipeline.auto_transforms as pauto
from sklearn.pipeline import Pipeline
import seldon.anomaly.AnomalyDetection as anod
import seldon.anomaly_wrapper as aw
import sys
import logging

def run_pipeline(events,models):

    tAuto = pauto.Auto_transform(max_values_numeric_categorical=2,exclude=["label"])
    detector = anod.iNNEDetector(sample_size=16)

    wrapper = aw.AnomalyWrapper(clf=detector,excluded=["label"])

    transformers = [("tAuto",tAuto),("clf",wrapper)]
    p = Pipeline(transformers)

    pw = sutl.Pipeline_wrapper()
    df = pw.create_dataframe(events)
    df2 = p.fit_transform(df)
    pw.save_pipeline(p,models)


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

