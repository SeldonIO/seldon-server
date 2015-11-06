import pandas as pd
from sklearn.cross_validation import KFold
from sklearn.metrics import accuracy_score
import seldon.pipeline.pipelines as pl
from sklearn.base import BaseEstimator

class Seldon_KFold(pl.Estimator,pl.Feature_transform,BaseEstimator):

    def __init__(self,clf=None,k=3):
        self.clf = clf
        self.k = k

    def get_models(self):
        return self.clf.get_models()

    def set_models(self,models):
        self.clf.set_mdoels(models)

    def save_model(self,folder_prefix):
        self.clf.save_model(folder_prefix)

    def load_model(self,folder_prefix):
        self.clf.load_model(folder_prefix)

    def get_scores(self):
        return self.scores

    def fit(self,X,y=None):
        df_len = X.shape[0]
        kf = KFold(df_len, n_folds=self.k,shuffle=True)
        self.scores = []
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
            self.scores.append(accuracy_score(y_test, y_pred))
        print "accuracy scores ",self.scores
        self.clf.fit(X,y)

    def predict_proba(self, X):
        self.clf.predict_proba(X)
