import numpy as np
import pandas as pd
import scipy.spatial.distance as ssd
from sklearn.utils import check_array
import logging
from time import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.propagate = False

class iNNEDetector(object):
    """
    Create an ensemble classifier for anomaly detection based on iNNE method (cite iNNE paper)
    
    Parameters
    ----------
    ensemble_size : int
        Number of ensembles for the classifier
    sample_size : int
        Number of samples on each ensemble
    metric : str
        Metric used by iNNE. Default 'euclid'
    verbose : bool
        default True
    """
    def __init__(self,ensemble_size=100,sample_size=32,metric='euclid',verbose=True):
        self.ensemble_size = ensemble_size
        self.sample_size = sample_size
        self.metric = metric
        self.verbose = verbose
        
    def _D(self,x,y,metric):
        """ 
        Calculates the distance between x and y according to metric 'metric'
        
        Parameters
        ----------
        
        x : numpy array 
            1-d vector of dimension d
        y : numpy array
            1-d vector of dimension d
        metric: str 
            specify the metric used (default euclidian metric)
        
        Returns
        -------
        
        D(x | y) : Distance between x and y according to metric 
        """
        
        if metric == 'euclid' or metric == 'Euclid':
            return np.linalg.norm(x-y)
        if metric == 'kolmogorov' or metric == 'Kolmogorov':
            #check normalization
            norm_x = np.around(np.linalg.norm(x),decimals=10)
            norm_y = np.around(np.linalg.norm(y),decimals=10)
            if norm_x == 1 and norm_y == 1:
                return np.sqrt(1 - np.around(np.absolute(np.dot(x,y))),decimals=10)
            else:
                raise NameError('%s metric supports only normalized vectors'
                                % metric)
        if metric == 'chebyshev' or metric == 'Chebyshev':
            return ssd.chebyshev(x,y)

        else:
            raise NameError('%s metric not supported'
                            % metric)

    def _generate_spheres(self,X_s):
        """ 
        Generates set of hyperspheres from sample X_s

        Parameters
        ----------

        X_s : numpy array 
            dimensions: sample_size X nb_features

        Returns
        -------
        
        spheres : list 
            list of tuples storing sphere's center, radius and nearest neighbour index
        
        """
        spheres = []
        for i in range(X_s.shape[0]):
            k = int(np.random.randint(X_s.shape[0],size=1))
            while k==i:
                k = int(np.random.randint(X_s.shape[0],size=1))
            radius = self._D(X_s[i],X_s[k],self.metric)
            nn_index = k
            for j in range(X_s.shape[0]):
                if self._D(X_s[i],X_s[j],self.metric) < radius and j!=i:
                    radius = self._D(X_s[i],X_s[j],self.metric)
                    nn_index = j
            spheres.append((X_s[i], radius, nn_index))
        
        return spheres

    def _score(self,y,spheres):
        """
        Returns the anomaly score for vector y based on the given  set of spheres
          
        Parameters
        ----------

        y : numpy array
            1-d vector of dimension d to score
        spheres : list
            list of 3-d tuples where each tuple contain sphere center, radius and nearest neighbour index

        Returns
        -------

        score : float
            anomaly score
        """
        spheres_in=[]
        for sphere in spheres:
            if self._D(y,sphere[0],self.metric) <= sphere[1]:
                spheres_in.append(sphere)
        if len(spheres_in) == 0:
            B = ()
        elif len(spheres_in) != 0:
            B = spheres_in[int(np.random.randint(len(spheres_in),size=1))]
            for sphere_in in spheres_in:
                if sphere_in[1] < B[1]:
                  B = sphere_in
                        
        if B == ():
            score = 1
        else:
            score = 1 - (float(spheres[B[2]][1])/float(B[1]))
    
        return score
  
    def fit(self,X,y=None):
        """ 
        Generates sets of hyper-spheres for anomaly scores 
        
        Parameters
        ----------
        
        X : numpy array (nb_samples, nb_features)
            data set
    
        Returns
        -------
        
        self
        """
        t_0 = time()
        
        check_array(X)
                 
        self._sets_of_spheres = []
        if self.verbose:
            logger.info('generating sets of spheres...')
        for j in range(self.ensemble_size):
            X_s = np.random.permutation(X)[:self.sample_size,:]
            spheres = self._generate_spheres(X_s)
            self._sets_of_spheres.append(spheres)
        t_f = time() - t_0
        m,s = divmod(t_f, 60)
        h,m = divmod(m, 60)
        if self.verbose:
            logger.info('Total run time: %i:%i:%i'
                        % (h,m,s))

        return self


    def fit_transform(self,X,y=None):
        """ 
        Generates sets of hyper-spheres for anomaly scores 
        
        Parameters
        ----------
        
        X : numpy array (nb_samples, nb_features)
            data set
    
        Returns
        -------
        
        self
        """
        t_0 = time()
        
        check_array(X)
                 
        self._sets_of_spheres = []
        if self.verbose:
            logger.info('generating sets of spheres...')
        for j in range(self.ensemble_size):
            X_s = np.random.permutation(X)[:self.sample_size,:]
            spheres = self._generate_spheres(X_s)
            self._sets_of_spheres.append(spheres)
        t_f = time() - t_0
        m,s = divmod(t_f, 60)
        h,m = divmod(m, 60)
        if self.verbose:
            logger.info('Total run time: %i:%i:%i'
                        % (h,m,s))

        return self
      
    def fit_score(self,X,y=None):
        """
        Generate set of hyper-sphere and return anomaly score for all points in dataset

        Parameters
        ----------

        X : numpy array
            data set

        Return
        ------

        scores : numpy array
            1-d vector with the anomaly scores for all data points
        """
        t_0 = time()

        check_array(X)
   
        self._sets_of_spheres = []
        if self.verbose:
            logger.info('generating sets of spheres...')
        for j in range(self.ensemble_size):
            X_s = np.random.permutation(X)[:self.sample_size,:]
            spheres = self._generate_spheres(X_s)
            self._sets_of_spheres.append(spheres)
    
        scores = np.zeros(X.shape[0])
        for i in range(X.shape[0]):
            if i % 1000 == 0 and self.verbose:
                logger.info('Getting anomaly score for data point %i'
                            % i)
                logger.info('X shape: %i X %i'
                            % X.shape)
            scores_i = []
            j=0
            for spheres in self._sets_of_spheres:
                score = self._score(X[i],spheres)
                if i % 1000 == 0 and j % 10 ==0 and self.verbose:
                    logger.info('Anomaly score for data point %i from estimator %i: %f'
                                % (i,j,score))
                scores_i.append(score)
                j+=1
            scores[i] = np.mean(scores_i)

        if 'X_scored' not in dir(self):
            self.X_scored = np.column_stack((X,scores))
        
        t_f = time() - t_0
        m,s = divmod(t_f, 60)
        h,m = divmod(m, 60)
        if self.verbose:
            logger.info('Total run time: %i:%i:%i'
                        % (h,m,s))

        return scores
    
    def get_all_scores(self):
        """ 
        Returns the dataset with the anomaly scores stored in the last column
    
        Parameters
        ----------
        
        None
        
        Returns
        -------

        X_scored : numpy array 
            the dataset with anomaly scores stored in the last column
        """
        if 'X_scored' in dir(self):
            return self.X_scored
        else:
            raise NameError('method get_all_scores returns scores only if method fit_score has been previously called')
            return self


    def get_score(self,X):
        """ 
        Calculates the anomaly score for a new data point X
    
        Parameters
        ----------

        y : numpy array
            1-d vector to score

        Returns
        -------
        
        score : tuple
            tuple where first element is the anomaly score and the second element is True if the point is lab            elled as anomalous and False if is labelled as non-anomalous based on the decision threshold
        """
        if X.ndim == 1:
            s = np.zeros(2)
            scores = []
            for spheres in self._sets_of_spheres:
                score_s = self._score(X,spheres)
                scores.append(score_s)
            score_mean = np.mean(scores)
            s[0]=score_mean
            s[1]=1-score_mean
            return s
        elif X.ndim == 2:
            s = np.zeros((X.shape[0],2))
            for i in range(X.shape[0]):
                scores = []
                for spheres in self._sets_of_spheres:
                    score_s = self._score(X,spheres)
                    scores.append(score_s)
                score_mean = np.mean(scores)
                s[i,0] = score_mean
                s[i,1] = 1-score_mean
            return s
        
    def get_anomalies(self,decision_threshold=1):
        """
        Returns the data points whose anomaly score is above the decision_threshold

        Parameters
        ----------

        decition_threshold : float
            anomaly decision threshold. Default 0.5

        Returns
        -------

        X_anom: numpy array (nb_anomalies, nb_features + 1)
            anomalous data points with anomaly scores stored in the last column
        """
        if 'X_scored' in dir(self):
            X_tmp = self.X_scored[:,:-1]
            scores_tmp = self.X_scored[:,-1]
            X_an = X_tmp[scores_tmp>=decision_threshold]
            anom_scores = scores_tmp[scores_tmp>=decision_threshold]
            self.X_anom = np.column_stack((X_an,anom_scores))
            return self.X_anom
        else:    
            raise NameError('method get_anomalies returns scores only if method fit_score has been previously called')
            return self


