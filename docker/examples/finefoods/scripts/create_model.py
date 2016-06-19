import sys, getopt, argparse
import seldon.fileutil as fu
import seldon.pipeline.basic_transforms as bt
import seldon.pipeline.tfidf_transform as ptfidf
import seldon.pipeline.util as sutl
import seldon.sklearn_estimator as ske
from sklearn.pipeline import Pipeline
import seldon.pipeline.cross_validation as cf
from sklearn.externals import joblib
import seldon.xgb as xg
import logging
import sys
import numpy as np

logger = logging.getLogger(__name__)

class XGBoostModel(object):
    
    def __init__(self,data_folder,model_folder):
        self.data_folder = data_folder
        self.model_folder = model_folder

    def train(self,sample):

        tTfidf = ptfidf.Tfidf_transform(input_feature="review",output_feature="tfidf",target_feature="sentiment",min_df=10,max_df=0.7,select_features=False,topn_features=50000,stop_words="english",ngram_range=[1,2])


        tFilter2 = bt.Include_features_transform(included=["tfidf","sentiment"])

        svmTransform = bt.Svmlight_transform(output_feature="svmfeatures",excluded=["sentiment"],zero_based=False)

        classifier_xg = xg.XGBoostClassifier(target="sentiment",svmlight_feature="svmfeatures",silent=1,max_depth=5,n_estimators=200,objective='binary:logistic',scale_pos_weight=0.2)

        cv = cf.Seldon_KFold(classifier_xg,metric='auc',save_folds_folder="./folds")
    
        transformers = [("tTfidf",tTfidf),("tFilter2",tFilter2),("svmTransform",svmTransform),("cv",cv)]

        p = Pipeline(transformers)

        pw = sutl.Pipeline_wrapper()
        df = pw.create_dataframe([self.data_folder],df_format="csv")
        if sample < 1.0:
            logger.info("sampling dataset to size %s ",sample)
            df = df.sample(frac=sample,random_state=1)
        
        logger.info("Data frame shape %d , %d",df.shape[0],df.shape[1])

        df2 = p.fit_transform(df)
        pw.save_pipeline(p,self.model_folder)
        logger.info("cross validation scores %s",cv.get_scores())

        return p

if __name__ == '__main__':
    import logging
    logger = logging.getLogger()
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(name)s : %(message)s', level=logging.DEBUG)
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser(prog='pipeline_example')
    parser.add_argument('--data', help='data folder', required=True)
    parser.add_argument('--model', help='model output folder', required=True)
    parser.add_argument('--sample', help='run on sample of raw data', default=1.0, type=float)

    args = parser.parse_args()
    opts = vars(args)


    x = XGBoostModel(data_folder=args.data,model_folder=args.model)
    x.train(args.sample)




