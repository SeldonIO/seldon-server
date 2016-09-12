import csv
import numpy as np
import os
import matplotlib.pyplot as plt
from matplotlib.mlab import griddata

class BaseAnomalyDetection():
  """
  """
  def _presample_csv(self,path_fin, path_fout, nb_samples_in, nb_samples_out):
    """ Samples nb_samples_out lines from a csv file with nb_samples_in lines
    
    Args:
    path_fin: str, path to the input file
    path_fout: str, destination  path for the output file
    nb_samples_in: int, number of lines in input file
    nb_samples_out: int, number of lines to sample
    
    Returns:
    path_fout: output file path
    """
    if type(nb_samples_in)!=int or type(nb_samples_out)!=int or nb_samples_in<=0 or nb_samples_out<=0:
      raise NameError('nb_samples_in and nb_samples_out must be positive  integers')
    if nb_samples_in < nb_samples_out:
      raise NameError('nb_samples_out must be smaller than nb_samples_in: nb_samples_out=%i, nb_samples_in=%i'
                      % (nb_samples_out, nb_samples_in))
    else:
      ran = np.random.permutation(range(1,nb_samples_in))[:,nb_samples_out]
      ran_list = [int(r[i]) for i in range(len(ran))]
      with open(path_fin, 'r') as fin:
        with open(path_fout, 'w') as fout:
          reader = csv.reader(fin)
          writer = csv.writer(fout)
          counter = 0
          for line in reader:
            if counter in ran_list or counter == 0:
              writer.writerow(line)
              counter += 1
          fout.close()
        fin.close()
      return path_fout
    
class BaseVisualizer():
  """
  """
  def _to_txt(self,X,path_X_fout):
    """
    """
    if os.path.exists(path_X_fout):
      os.remove(path_X_fout)
    with open(path_X_fout, 'w') as X_fout:
      X_fout.write('%i %i\n' % (int(X.shape[0]),int(X.shape[1])))
      for i in range(X.shape[0]):
        x_tup = tuple(X[i,:].tolist())
        x_writer = '0.4%f '*X.shape[1]+'\n'
        X_fout.write(x_writer % x_tup)
      X_fout.close()
    return path_X_fout
  
  def _scatter_plot(self, points, labels='', img_path_fout=''):
    """
    """
    if labels=='':
      plt.plot(points[:,0],points[:,1], 'r.', markersize=2)
      if img_path_fout!='':
        plt.savefig(img_path_fout)
      plt.show()
    elif labels!='':
      labeled_points = {}
      for i in range(points.shape[0]):
        labeled_points.setdefault(labels[i],[]).append((points[i,0],points[i,1]))
      colors = plt.cm.rainbow(np.linspace(0,1,len(labeled_points)))
      for color, ll in zip(colors,sorted(labeled_points.keys())):
        x = [t[0] for t in labeled_points[ll]]
        y = [t[1] for t in labeled_points[ll]]
        if ll!=0:
          plt.plot(x,y,'.', color = color, markersize = 2)
        elif ll==0:
          plt.plot(x,y,'o', color = color, markersize = 2)
      if img_path_fout!='':
        plt.savefig(img_path_fout)
      plt.show()
  
  def _contour_plot(self, points, labels='', img_path_fout='', contour_values='',
                    show_points = True):
    #TO BE COMPLETED
    """
    """

    labeled_points = {}
    for i in range(points.shape[0]):
      labeled_points.setdefault(labels[i],[]).append((points[i,0],points[i,1]))
    colors = plt.cm.rainbow(np.linspace(0,1,len(labeled_points)))
      
    x, y, z = points[:,0], points[:,1], contour_values
    min_x, max_x, min_y, max_y = x.min(),x.max(),y.min(),y.max()
    xi = np.linspace(min_x-0.1,max_x+0.1,points.shape[0])
    yi = np.linspace(min_y-0.1,max_y+0.1,points.shape[0])
    zi = griddata(x,y,z,xi,yi,interp='linear')
#    CS = plt.contour(xi, yi, zi, 15, linewidths=0.5, colors='k')
    CS = plt.contourf(xi, yi, zi, 15, cmap=plt.cm.gray, vmax=abs(zi).max(), vmin=-abs(zi).max())
    plt.colorbar()
    if show_points:
      for color, ll in zip(colors,sorted(labeled_points.keys())):
        x_1 = [t[0] for t in labeled_points[ll]]
        y_1 = [t[1] for t in labeled_points[ll]]
        if ll!=0:
          plt.plot(x_1,y_1,'.', color = color, markersize = 2)
        elif ll==0:
          plt.plot(x_1,y_1,'^', color = color, markersize = 2)
    plt.title('title')
  
    if img_path_fout!='':
      plt.savefig(img_path_fout)
    plt.show()
