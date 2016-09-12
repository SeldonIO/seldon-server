import numpy as np
import logging
from Base import BaseAnomalyDetection
from time import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
    
class iNNEDetector(BaseAnomalyDetection):
  """
  """
  def __init__(self,ensemble_size=100,sample_size=16,metric='euclid',verbose=True):
    self.ensemble_size = ensemble_size
    self.sample_size = sample_size
    self.metric = metric
    self.verbose = verbose
  
  def _D(self,x,y,metric):
    """ Calculates the distance between x and y according to metric 'metric'
    
    Args: 
    x: numpy vector of dimension d
    y: numpy vector of dimension d
    metric: str, specify the metric used (default euclidian metric)
    
    Returns:
    D(x | y)
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
    if metric == 'chebyshev':
      raise NameError('%s metric not supported yet'
                      % metric)
    if metric == 'hyper-spherical':
      raise NameError('%s metric not supported yet'
                      % metric)
    if metric == 'hyper-rectangular':
      raise NameError('%s metric not supported yet'
                      % metric)
    else:
      raise NameError('Wwhat is %s? Are you still drunk from last night?'
                      % metric)

  def _generate_spheres(self,X_s):
    """ Generates set of hyperspheres from sample X_s
    
    Args:
    X_s: numpy array, dimensions: sample_size X nb_features
    
    Returns:
    spheres: list of tuples storing sphere's center, radius and nearest neighbour index
        
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
  
        
  def fit(self,X):
    """ Generates sets of hyper-spheres for anomaly scores 
        
    Args:
    X: numpy array
    
    Returns:
    self
    """
    t_0 = time()
    self.X = X
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
  
  def fit_score(self,X):
    """
    """
    t_0 = time()
    
    self._sets_of_spheres = []
    if self.verbose:
      logger.info('generating sets of spheres...')
    for j in range(self.ensemble_size):
      X_s = np.random.permutation(X)[:self.sample_size,:]
      spheres = self._generate_spheres(X_s)
      self._sets_of_spheres.append(spheres)
    
    self.scores = np.zeros(X.shape[0])
    for i in range(X.shape[0]):
      if i % 100 == 0 and self.verbose:
        logger.info('Getting anomaly score for data point %i'
                    % i)
        logger.info('X shape: %i X %i'
                    % X.shape)
      scores_i = []
      for spheres in self._sets_of_spheres:
        score = self._score(X[i],spheres)
        if i % 100 == 0 and self.verbose:
          logger.info('Anomaly score for data point %i from estimator %i: %f'
                      % (i,j,score))
        scores_i.append(score)
      self.scores[i] = np.mean(scores_i)
      
    t_f = time() - t_0
    m,s = divmod(t_f, 60)
    h,m = divmod(m, 60)
    if self.verbose:
      logger.info('Total run time: %i:%i:%i'
                  % (h,m,s))
        
    return self.scores

  def get_all_scores(self):
    """ Returns the dataset with the anomaly scores stored in the last column
    
    Args: 
    self
        
    Returns:
    X_scored: numpy array, the dataset with anomaly scores stored in the last column
    """
        
    scores = np.zeros(self.X.shape[0])
    for i in range(self.X.shape[0]):
      if i % 100 == 0 and self.verbose:
        logger.info('Getting anomaly score for data point %i'
                    % i)
        logger.info('X shape: %i X %i'
                    % self.X.shape)
      scores_i = []
      j=0
      for spheres in self._sets_of_spheres:
        j += 1
        score = self._score(self.X[i],spheres)
        if i % 100 == 0 and self.verbose:
          logger.info('Anomaly score for data point %i from estimator %i: %f'
                      % (i,j,score))
        scores_i.append(score)
      scores[i] = np.mean(scores_i)
    self.X_scored = np.column_stack((self.X,scores))

    return self.X_scored
    
  def get_score(self,y):
    """ Calculates the anomaly score for a new data point y
    
    Args:
    y: numpy vector

    Returns:
    score
    """
    scores = []
    for spheres in self._sets_of_spheres:
      score_s = self._score(y,spheres)
      scores.append(score_s)
    score = np.mean(scores)

    return score
        
  

        
    
    
