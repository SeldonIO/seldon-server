import pandas as pd
import numpy as np
import tensorflow as tf
from sklearn import preprocessing
from seldon.tensorflow_wrapper import TensorFlowWrapper
from sklearn.pipeline import Pipeline
import seldon.pipeline.util as sutl
import argparse
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

df_Xy = pd.read_csv('home/seldon/data/indicators_nan_replaced.csv')
#df_Xy = pd.read_csv('home/seldon/data/indicators_nan_replacedv2.csv')
#df_Xy = pd.read_csv('home/seldon/data/indicators_nan_replacedv3.csv')

logger.info('tf version: %s ' % tf.__version__)

def get_data(split_train_test=False):
    
    cols_tokeep = df_Xy.columns.tolist()
    logger.debug('columns to keep: ')
    logger.debug(cols_tokeep)
    
    if 'company_id' in cols_tokeep:
        cols_tokeep.remove('company_id')
    Xy = df_Xy.as_matrix(columns=cols_tokeep)
    Xy_shuffled = np.random.permutation(Xy)

    (means,stds) = (np.mean(Xy_shuffled[:,:-1],axis=0).reshape((1,Xy_shuffled[:,:-1].shape[1])),
                    np.std(Xy_shuffled[:,:-1],axis=0).reshape((1,Xy_shuffled[:,:-1].shape[1])))
            
    for i,v in enumerate(means[0]):
        if i%2!=0:
            means[0,i]=0
            stds[0,i]=1

    if split_train_test:
        #split train-test
        split_ratio = int(0.7*(len(Xy)))
        Xy_train = Xy_shuffled[:split_ratio,:]
        Xy_test = Xy_shuffled[split_ratio:,:]
                        
        dataset = {'train':Xy_train, 'test':Xy_test, 'means':means, 'stds':stds}

    else:
        #no splitting                
        dataset = {'train':Xy_shuffled, 'test':Xy_shuffled, 'means':means, 'stds':stds}

    return dataset

def get_data_v3(split_train_test=False):
    
    cols_tokeep = df_Xy.columns.tolist()
    logger.debug('columns to keep')
    logger.debug(cols_tokeep)

    if 'company_id' in cols_tokeep:
        cols_tokeep.remove('company_id')
    Xy = df_Xy.as_matrix(columns=cols_tokeep)
    Xy_shuffled = np.random.permutation(Xy)

    (means,stds) = (np.mean(Xy_shuffled[:,:-1],axis=0).reshape((1,Xy_shuffled[:,:-1].shape[1])),
                    np.std(Xy_shuffled[:,:-1],axis=0).reshape((1,Xy_shuffled[:,:-1].shape[1])))
            

    if split_train_test:
        #split train-test
        split_ratio = int(0.7*(len(Xy)))
        Xy_train = Xy_shuffled[:split_ratio,:]
        Xy_test = Xy_shuffled[split_ratio:,:]
                        
        dataset = {'train':Xy_train, 'test':Xy_test, 'means':means, 'stds':stds}

    else:
        #no splitting                
        dataset = {'train':Xy_shuffled, 'test':Xy_shuffled, 'means':means, 'stds':stds}

    return dataset


def fill_feed_dict_train(in_pl,
                         y_pl,
                         dataset,
                         iterator,
                         batch_size=128):

    train = dataset['train']

    if batch_size=='all':
        feed_dict_train = {in_pl : train[:,:-1],
                           y_pl : train[:,-1].reshape((len(train),1))}

    else:
        nb_batches = int(dataset['train'].shape[0]/batch_size)
        j = iterator % nb_batches
    
        feed_dict_train = {in_pl : train[j*batch_size:(j+1)*batch_size,:-1],
                           y_pl : train[j*batch_size:(j+1)*batch_size,-1].reshape((batch_size,1))}
    
    return feed_dict_train

def fill_feed_dict_test(in_pl,
                        y_pl,
                        dataset):
    test = dataset['test']

    feed_dict_test = {in_pl : test[:,:-1],
                      y_pl : test[:,-1].reshape((len(test),1))} 

    return feed_dict_test

dataset = get_data()
#dataset = get_data_v3()

# model v1
def create_pipeline_v1(load=None):
    
    nb_features = 58
    nb_hidden1 = 116
    nb_hidden2 = 29

    batch_size = 64
    nb_iter = 30001
    lamb = 0.0001
    
    in_pl = tf.placeholder(dtype=tf.float32,
                           shape=(None,nb_features),
                           name='input_placeholder')

    means = tf.constant(dataset['means'],
                           dtype=tf.float32,
                           shape=(1,nb_features),
                           name='features_means')
    stds = tf.constant(dataset['stds'],
                       dtype=tf.float32,
                       shape=(1,nb_features),
                       name='features_stds_placeholder')
    means_tiled = tf.tile(means,[tf.shape(in_pl)[0],1])
    stds_tiled = tf.tile(stds,[tf.shape(in_pl)[0],1])    

    #scaled inputs
    inp = (in_pl - means_tiled)/(stds_tiled+1e-10)

    y_pl = tf.placeholder(dtype=tf.float32,
                          shape=(None,1),
                          name='target_placeholder')

    
    W1 = tf.Variable(tf.truncated_normal([nb_features,nb_hidden1]),
                     dtype=tf.float32,
                     name='first_layer_weights')
    W1_L2reg = (1/2*batch_size)*tf.reduce_sum(tf.square(W1))
    b1 = tf.Variable(tf.zeros(shape=[nb_hidden1]))

    #first hidden layer
    h1 = tf.nn.relu(tf.matmul(inp,W1) + b1,
                    name='first_hidden_layer')

    W2 = tf.Variable(tf.truncated_normal([nb_hidden1,nb_hidden2]),
                     dtype=tf.float32,
                     name='second_layer_weights')
    W2_L2reg = (1/2*batch_size)*tf.reduce_sum(tf.square(W2))
    b2 = tf.Variable(tf.zeros(shape=[nb_hidden2]))

    #second hidden layer
    h2 = tf.sigmoid(tf.matmul(h1,W2) + b2,
                    name='second_hidden_layer')

    W3 = tf.Variable(tf.truncated_normal([nb_hidden2,1]),
                     dtype=tf.float32,
                     name='last_layer_weights')
    W3_L2reg = (1/2*batch_size)*tf.reduce_sum(tf.square(W3))
    b3 = tf.Variable(tf.zeros(shape=[1]))

    #out layer
    out = tf.sigmoid(tf.matmul(h2,W3) + b3,
                     name='output_layer')
    proba = tf.squeeze(tf.pack([1-out,out],2),
                       squeeze_dims=[1])

    L2reg = lamb*(W1_L2reg + W2_L2reg + W3_L2reg)
                  
    cross_entropy = -(1/float(2))*tf.reduce_mean(y_pl * tf.log(out+1e-10) + (1-y_pl) * tf.log(1-out+1e-10),
                                                 name='cost_function')
    cost = cross_entropy + L2reg
    train_step = tf.train.AdamOptimizer(1e-4).minimize(cost)

    init = tf.initialize_all_variables()
    sess = tf.Session()
    
    logger.info('Training model...')
    logger.info('model version: %i' % 1)
    sess.run(init)
    
    for i in range(nb_iter):

        if i % 1000 == 0:
            logger.info('iteration %i of %i' % (i,nb_iter))
            
        feed_dict_train = fill_feed_dict_train(in_pl,
                                               y_pl,
                                               dataset,
                                               i,
                                               batch_size=batch_size)
        (_,
         W3_value,
         cost_value,
         out_value) = sess.run([train_step,
                                W3,
                                cost,
                                out],
                               feed_dict=feed_dict_train)

        if i % 10000 == 0:
#            feed_dict_test = fill_feed_dict_test(in_pl,
#                                                 y_pl,
#                                                 dataset)
            
            inp_values,proba_values = sess.run([inp,proba],feed_dict=feed_dict_train)
            logger.debug('scaled inputs:')
            logger.debug(inp_values)
            logger.debug('probabilities:')
            logger.debug(proba_values)
            logger.debug('proba out shape:')
            logger.debug(proba_values.shape)
            logger.debug('cost: %f' % cost_value)

    tfw = TensorFlowWrapper(sess,tf_input=in_pl,tf_output=proba,
                            target="y",target_readable="class",excluded=['class'])

    return Pipeline([('deep_classifier',tfw)])

#model v2
def create_pipeline_v2(load=None):
    
    nb_features = 58
    nb_hidden1 = 400
    nb_hidden2 = 200
    nb_hidden3 = 100
    
    batch_size = 64
    nb_iter = 30001
    lamb = 0.0001
    
    in_pl = tf.placeholder(dtype=tf.float32,
                           shape=(None,nb_features),
                           name='input_placeholder')

    means = tf.constant(dataset['means'],
                           dtype=tf.float32,
                           shape=(1,nb_features),
                           name='features_means')
    stds = tf.constant(dataset['stds'],
                       dtype=tf.float32,
                       shape=(1,nb_features),
                       name='features_stds_placeholder')
    means_tiled = tf.tile(means,[tf.shape(in_pl)[0],1])
    stds_tiled = tf.tile(stds,[tf.shape(in_pl)[0],1])    

    #scaled inputs
    inp = (in_pl - means_tiled)/(stds_tiled+1e-10)

    y_pl = tf.placeholder(dtype=tf.float32,
                          shape=(None,1),
                          name='target_placeholder')

    #first hidden layer    
    W1 = tf.Variable(tf.truncated_normal([nb_features,nb_hidden1]),
                     dtype=tf.float32,
                     name='first_layer_weights')
    W1_L2reg = (1/2*batch_size)*tf.reduce_sum(tf.square(W1))
    b1 = tf.Variable(tf.zeros(shape=[nb_hidden1]))
    
    h1 = tf.sigmoid(tf.matmul(inp,W1) + b1,
                    name='first_hidden_layer')

    #second hidden layer
    W2 = tf.Variable(tf.truncated_normal([nb_hidden1,nb_hidden2]),
                     dtype=tf.float32,
                     name='second_layer_weights')
    W2_L2reg = (1/2*batch_size)*tf.reduce_sum(tf.square(W2))
    b2 = tf.Variable(tf.zeros(shape=[nb_hidden2]))

    h2 = tf.sigmoid(tf.matmul(h1,W2) + b2,
                    name='second_hidden_layer')

    #third hidden layer
    W3 = tf.Variable(tf.truncated_normal([nb_hidden2,nb_hidden3]),
                     dtype=tf.float32,
                     name='third_layer_weights')
    W3_L2reg = (1/2*batch_size)*tf.reduce_sum(tf.square(W3))
    b3 = tf.Variable(tf.zeros(shape=[nb_hidden3]))

    h3 = tf.sigmoid(tf.matmul(h2,W3) + b3,
                    name='third_hidden_layer')

    #out layer
    W4 = tf.Variable(tf.truncated_normal([nb_hidden3,1]),
                     dtype=tf.float32,
                     name='last_layer_weights')
    W4_L2reg = (1/2*batch_size)*tf.reduce_sum(tf.square(W4))
    b4 = tf.Variable(tf.zeros(shape=[1]))

    out = tf.sigmoid(tf.matmul(h3,W4) + b4,
                     name='output_layer')
    proba = tf.squeeze(tf.pack([1-out,out],2),
                       squeeze_dims=[1])

    L2reg = lamb*(W1_L2reg + W2_L2reg + W3_L2reg + W4_L2reg)
                  
    cross_entropy = -(1/float(2))*tf.reduce_mean(y_pl * tf.log(out+1e-10) + (1-y_pl) * tf.log(1-out+1e-10),
                                                 name='cost_function')
    cost = cross_entropy + L2reg
    train_step = tf.train.AdamOptimizer(1e-4).minimize(cost)

    init = tf.initialize_all_variables()
    sess = tf.Session()
    
    logger.info('Training model...')
    logger.info('model version: %i' % 2)

    sess.run(init)
    
    for i in range(nb_iter):

        if i % 1000 == 0:
            logger.info('iteration %i of %i' % (i,nb_iter))
            
        feed_dict_train = fill_feed_dict_train(in_pl,
                                               y_pl,
                                               dataset,
                                               i,
                                               batch_size=batch_size)
        (_,
         W3_value,
         cost_value,
         out_value) = sess.run([train_step,
                                W3,
                                cost,
                                out],
                               feed_dict=feed_dict_train)

        if i % 10000 == 0:
#            feed_dict_test = fill_feed_dict_test(in_pl,
#                                                 y_pl,
#                                                 dataset)
            
            inp_values,proba_values = sess.run([inp,proba],feed_dict=feed_dict_train)
            logger.debug('scaled inputs')
            logger.debug(inp_values)
            logger.debug('probabilities')
            logger.debug(proba_values)
            logger.debug('proba out shape')
            logger.debug(proba_values.shape)
            logger.debug('cost')
            logger.debug(cost_value)

    tfw = TensorFlowWrapper(sess,tf_input=in_pl,tf_output=proba,
                            target="y",target_readable="class",excluded=['class'])

    return Pipeline([('deep_classifier',tfw)])

# model v3
def create_pipeline_v3(load=None):
    
    nb_features = 29
    nb_hidden1 = 400
    nb_hidden2 = 200
    nb_hidden3 = 100
    
    batch_size = 64
    nb_iter = 30001
    lamb = 0.0001
    
    in_pl = tf.placeholder(dtype=tf.float32,
                           shape=(None,nb_features),
                           name='input_placeholder')

    means = tf.constant(dataset['means'],
                           dtype=tf.float32,
                           shape=(1,nb_features),
                           name='features_means')
    stds = tf.constant(dataset['stds'],
                       dtype=tf.float32,
                       shape=(1,nb_features),
                       name='features_stds_placeholder')
    means_tiled = tf.tile(means,[tf.shape(in_pl)[0],1])
    stds_tiled = tf.tile(stds,[tf.shape(in_pl)[0],1])    

    #scaled inputs
    inp = (in_pl - means_tiled)/(stds_tiled+1e-10)

    y_pl = tf.placeholder(dtype=tf.float32,
                          shape=(None,1),
                          name='target_placeholder')

    #first hidden layer    
    W1 = tf.Variable(tf.truncated_normal([nb_features,nb_hidden1]),
                     dtype=tf.float32,
                     name='first_layer_weights')
    W1_L2reg = (1/2*batch_size)*tf.reduce_sum(tf.square(W1))
    b1 = tf.Variable(tf.zeros(shape=[nb_hidden1]))
    
    h1 = tf.sigmoid(tf.matmul(inp,W1) + b1,
                    name='first_hidden_layer')

    #second hidden layer
    W2 = tf.Variable(tf.truncated_normal([nb_hidden1,nb_hidden2]),
                     dtype=tf.float32,
                     name='second_layer_weights')
    W2_L2reg = (1/2*batch_size)*tf.reduce_sum(tf.square(W2))
    b2 = tf.Variable(tf.zeros(shape=[nb_hidden2]))

    h2 = tf.sigmoid(tf.matmul(h1,W2) + b2,
                    name='second_hidden_layer')

    #third hidden layer
    W3 = tf.Variable(tf.truncated_normal([nb_hidden2,nb_hidden3]),
                     dtype=tf.float32,
                     name='third_layer_weights')
    W3_L2reg = (1/2*batch_size)*tf.reduce_sum(tf.square(W3))
    b3 = tf.Variable(tf.zeros(shape=[nb_hidden3]))

    h3 = tf.sigmoid(tf.matmul(h2,W3) + b3,
                    name='third_hidden_layer')

    #out layer
    W4 = tf.Variable(tf.truncated_normal([nb_hidden3,1]),
                     dtype=tf.float32,
                     name='last_layer_weights')
    W4_L2reg = (1/2*batch_size)*tf.reduce_sum(tf.square(W4))
    b4 = tf.Variable(tf.zeros(shape=[1]))

    out = tf.sigmoid(tf.matmul(h3,W4) + b4,
                     name='output_layer')
    proba = tf.squeeze(tf.pack([1-out,out],2),
                       squeeze_dims=[1])

    L2reg = lamb*(W1_L2reg + W2_L2reg + W3_L2reg + W4_L2reg)
                  
    cross_entropy = -(1/float(2))*tf.reduce_mean(y_pl * tf.log(out+1e-10) + (1-y_pl) * tf.log(1-out+1e-10),
                                                 name='cost_function')
    cost = cross_entropy + L2reg
    train_step = tf.train.AdamOptimizer(1e-4).minimize(cost)

    init = tf.initialize_all_variables()
    sess = tf.Session()
    
    logger.info('Training model...')
    logger.info('model version %i' % 3)

    sess.run(init)
    
    for i in range(nb_iter):

        if i % 1000 == 0:
            logger.info('iteration %i of %i' % (i,nb_iter))
            
        feed_dict_train = fill_feed_dict_train(in_pl,
                                               y_pl,
                                               dataset,
                                               i,
                                               batch_size=batch_size)
        (_,
         W3_value,
         cost_value,
         out_value) = sess.run([train_step,
                                W3,
                                cost,
                                out],
                               feed_dict=feed_dict_train)

        if i % 10000 == 0:
#            feed_dict_test = fill_feed_dict_test(in_pl,
#                                                 y_pl,
#                                                 dataset)
            
            inp_values,proba_values = sess.run([inp,proba],feed_dict=feed_dict_train)
            logger.debug('scaled inputs')
            logger.debug(inp_values)
            logger.debug('probabilities')
            logger.debug(proba_values)
            logger.debug('proba out shape')
            logger.debug(proba_values.shape)
            logger.debug('cost')
            logger.debug(cost_value)

    tfw = TensorFlowWrapper(sess,tf_input=in_pl,tf_output=proba,
                            target="y",target_readable="class",excluded=['class'])

    return Pipeline([('deep_classifier',tfw)])


if __name__ == '__main__':

    parser = argparse.ArgumentParser(prog='pipeline_example')
    parser.add_argument('-m','--model', help='model output folder', required=True)
    parser.add_argument('-l','--load',help='Load pretrained model from file')
    
    args = parser.parse_args()
    
    p = create_pipeline_v1(args.load)
#    p = create_pipeline_v2(args.load)
#    p = create_pipeline_v3(args.load)

    pw = sutl.PipelineWrapper()

    pw.save_pipeline(p,args.model)

    logger.info('tf version: %s' % tf.__version__)
    logger.info('pipeline saved in %s' % args.model)
