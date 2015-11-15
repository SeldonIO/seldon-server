import pandas as pd
from sklearn.cross_validation import KFold
from sklearn.metrics import accuracy_score
from sklearn.base import BaseEstimator

class Seldon_KFold(BaseEstimator):
    """
    Simple wrapper to provide cross validation test using estimator with input from pandas dataframe

    Parameters
    ----------

    clf : object
       Pandas compatible scikit learn Estimator to apply to data splits
    k : int, optional
       number of folds
    """
    def __init__(self,clf=None,k=5):
        self.clf = clf
        self.k = k
        self.scores = []

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
