import logging
import numpy as np
import matplotlib.pyplot as plt
import LargeVis
from Base import BaseVisualizer
from sklearn.manifold import TSNE

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

class HighDVisualizer(BaseVisualizer):
  """
  """
  def __init__(self, method = 'LargeVis', outdim=2, fea=1, threads=-1, samples=-1,
               prop=-1, alpha=-1, trees=-1, neg=-1, neigh=-1,
               gamma=-1, perp=-1):
    self.method = method
    self.outdim = outdim
    self.fea = fea
    self.threads = threads
    self.samples = samples
    self.prop = prop
    self.alpha = alpha
    self.trees = trees
    self.neg = neg
    self.neigh = neigh
    self.gamma = gamma
    self.perp = perp
  
  def fit(self,X):
    """ Generates and returns low dimensional representation using LargeVis method and save it in txt format
    
    Args:
    X: numpy array, high dimensional representation
    labels: numpy vector, label for each data point. Optional in fit, default None
    returns
    X_dim_out: numpy array, low dimensional representation
    
    """
    if self.method=='LargeVis':
      self.path_X_dim_in = self._to_txt(X=X,path_X_fout='X_dim_in.txt')
      self.path_X_dim_out = 'X_dim_out.txt'
      LargeVis.loadfile(self.path_X_dim_in)
      Y = LargeVis.run(self.outdim, self.threads, self.samples, self.prop, self.alpha,
                       self.trees, self.neg, self.neigh, self.gamma, self.perp)
      LargeVis.save(self.path_X_dim_out)
      self.X_low_dim = np.asarray(Y)
    elif self.method=='t-SNE':
      tsne = TSNE(verbose=1)
      self.X_low_dim = tsne.fit_transform(X)
    else:
      raise NameError('method %s not supported. Supported methods: t-SNE or LargeVis')
    
    return self.X_low_dim
  
  def visualize(self, labels='', img_path_fout=''):
    """
    """
    if labels!='' and labels.shape[0]!=self.X_low_dim.shape[0]:
      raise NameError('number of data points and number of labels must be the same')
    else:
      self._scatter_plot(points=self.X_low_dim, labels=labels, img_path_fout=img_path_fout)

  def visualize_with_contour(self, labels='', img_path_fout='', contour_values=''):
    #TO BE COMPLETED
    """
    """
    self._contour_plot(self.X_low_dim, labels, img_path_fout, contour_values)
  



