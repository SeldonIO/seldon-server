import pandas as pd
from sklearn.cross_validation import KFold
from sklearn import metrics
from sklearn.base import BaseEstimator
import logging
import numpy as np

logger = logging.getLogger(__name__)

class Seldon_KFold(BaseEstimator):
    """
    Simple wrapper to provide cross validation test using estimator with input from pandas dataframe

    Parameters
    ----------

    clf : object
       Pandas compatible scikit learn Estimator to apply to data splits
    k : int, optional
       number of folds
    save_folder_folder : str, optional
       a folder to save prediction results from each fold
    """
    def __init__(self,clf=None,k=5,save_folds_folder=None,metric='accuracy',random_state=1):
        self.clf = clf
        self.k = k
        self.scores = []
        self.save_folds_folder=save_folds_folder
        self.metric = metric
        self.random_state = random_state

    def get_scores(self):
        return self.scores

    def get_score(self):
        if len(self.scores) > 0:
            return sum(self.scores) / float(len(self.scores))
        else:
            return 0.0


    def fit(self,X,y=None):
        """
        Split dataframe into k folds and train test classifier on each. Finally train classifier on all data.
        Parameters
        ----------

        X : pandas dataframe 

        Returns
        -------
        self: object
        """
        df_len = X.shape[0]
        kf = KFold(df_len, n_folds=self.k,shuffle=True,random_state=self.random_state)
        self.scores = []
        idx = 1
        for train_index, test_index in kf:
            if isinstance(X,pd.DataFrame):
                X_train = X.iloc[train_index]
                y_train = None
                X_test = X.iloc[test_index]
                y_test = X_test[self.clf.get_target()]
            else:
                X_train, X_test = X[train_index], X[test_index]
                y_train, y_test = y[train_index], y[test_index]
            self.clf.fit(X_train,y_train)
            y_pred = self.clf.predict(X_test)
            y_pred_proba = self.clf.predict_proba(X_test)
            if self.metric == 'accuracy':
                self.scores.append(metrics.accuracy_score(y_test, y_pred))
            elif self.metric == 'auc':
                fpr, tpr, thresholds = metrics.roc_curve(y_test, y_pred_proba[:, 1])
                self.scores.append(metrics.auc(fpr, tpr))
            logger.info("Running scores %s",self.scores)
            if not self.save_folds_folder is None:
                np.savetxt(self.save_folds_folder+"/"+str(idx)+"_correct.txt",y_test,fmt='%1.3f')
                np.savetxt(self.save_folds_folder+"/"+str(idx)+"_predictions.txt",y_pred,fmt='%1.3f')
                np.savetxt(self.save_folds_folder+"/"+str(idx)+"_predictions_proba.txt",y_pred_proba,fmt='%1.3f')
            idx += 1
        logger.info("accuracy scores %s",self.scores)
        self.clf.fit(X,y)
        return self

    def transform(self,X):
        """
        Do nothing and pass input back
        """
        return X

    def predict_proba(self, X):
        return self.clf.predict_proba(X)

    def get_class_id_map(self):
        return self.clf.get_class_id_map()

    def set_params(self,**params):
        self.clf.set_params(params)
