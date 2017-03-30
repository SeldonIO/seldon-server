import numpy as np
import sklearn as sk
import requests
import json
import scipy.misc
import time
import operator
import matplotlib.pyplot as plt
import scipy.spatial.distance as ssd
from sklearn import linear_model
from flask import jsonify, request

_globals={}

def encode_input(in_vector):
    data = [feature for i,feature in enumerate(in_vector)]
    return {'data':data}

def get_token():
    try:
        p = '{url}/token?consumer_key={key}&consumer_secret={secret}'.format(
            url=_globals['url'],
            key=_globals['key'],
            secret=_globals['secret'])
        r = requests.post(p)
        print 'token retrieved'
        print p
    
    except:
        print 'Failed to retrieve token'
        return ''
    
    return r.json().get('access_token')

def request_prediction(in_vector,token):
    req = requests.post('{url}/predict?oauth_token={token}'.format(url=_globals['url'],token=token),
                        json = encode_input(in_vector))
    return req

class Predictor():

    def __init__(self,
                 host,
                 key,
                 secret,
                 _globals=_globals):

        self._globals = _globals
        self._globals['url'] = host
        self._globals['key'] = key
        self._globals['secret'] = secret
        self._globals['token'] = get_token()
    
    def predict(self,
                in_vector):

        r = request_prediction(in_vector,
                               _globals['token'])
        if r.json().get('error_id') == 8: # Token expired
            _globals['token'] = get_token()
            r = request_prediction(request.json,
                                   _globals['token'])

        predictions = r.json()
#        print predictions
        list_preds = [(int(x['predictedClass']), float(x['prediction']))
                      for x in predictions['predictions']]
        list_preds.sort()
#        d_preds = {pred[0]:pred[1] for pred in list_preds}
        preds = [x[1] for x in list_preds]
#        pred = (preds.index(max(preds)),max(preds))
#        d_preds_json = json.dumps(d_preds)
        
        return preds

class Explainer():

    def __init__(self,
                 predictor_url,
                 predictor_key,
                 predictor_secret,
                 nb_samples=100,
                 perturbation=0.1,
                 sim_stddev=1):
        
        self.nb_samples = nb_samples
        self.perturbation = perturbation
        self.predictor_instance = Predictor(predictor_url,
                                            predictor_key,
                                            predictor_secret)
        self.sim_stddev = sim_stddev


    def get_features_scores(self,in_vector):
        
        self.features_scores = {}
        for i,f in enumerate(in_vector):
            if i % 10 == 0:
                print 'Getting score for feature %i of %i' % (i,len(in_vector))
            importance = 0
            samples_counter = 0
            pvector = np.copy(in_vector)
                               
            tpred1_0 = time.time()
            preds = self.predictor_instance.predict(pvector)
            tpred1_f = time.time()-tpred1_0
            if n % 10 == 0 and i % 10 == 0:
                print '    original:' 
                print '    time for first query to  microservice: %f' % tpred1_f
                print '    prediction:', preds.index(max(preds))
                logprob = -np.log(max(preds))
                pred_idx = preds.index(max(preds))
                    
            pvector[i] = 0
            tpred2_0 = time.time()
            preds_f0 = self.predictor_instance.predict(pvector)
            tpred2_f = time.time() - tpred2_0
            if n % 10 == 0 and i % 10 == 0:
                print '    time for second query to microservice: %f' % tpred2_f
                print '    prediction:', preds.index(max(preds))
            logprob_f0 = -np.log(preds_f0[pred_idx])

            try:
                importance += np.abs((logprob - logprob_f0)/logprob)
            except ZeroDivisionError:
                logprob = 0.0000000001
                importance += np.abs((logprob - logprob_f0)/logprob)
                
            samples_counter += 1
            
            for n in range(self.nb_samples):
                ttot_0 = time.time()
                pvector = np.copy(in_vector)
                random_idxs = np.random.choice(len(in_vector),
                                                size = int(self.perturbation*len(in_vector)),
                                                replace=False)

                if i not in random_idxs:
                    random_values = np.asarray([np.random.normal(in_vector[idx],
                                                                 self.sim_stddev)
                                                for idx in random_idxs]) 
                    pvector[random_idxs] = random_values
                    
                    tpred1_0 = time.time()
                    preds = self.predictor_instance.predict(pvector)
                    tpred1_f = time.time()-tpred1_0
                    if n % 10 == 0 and i % 10 == 0:
                        print '    samples generated: %i of %i' % (n,self.nb_samples)
                        print '    time for first query to  microservice: %f' % tpred1_f
                        print '    prediction:', preds.index(max(preds))
                    logprob = -np.log(max(preds))
                    pred_idx = preds.index(max(preds))
                    
                    pvector[i] = 0
                    tpred2_0 = time.time()
                    preds_f0 = self.predictor_instance.predict(pvector)
                    tpred2_f = time.time() - tpred2_0
                    if n % 10 == 0 and i % 10 == 0:
                        print '    time for second query to microservice: %f' % tpred2_f
                        print '    prediction:', preds.index(max(preds))
                    logprob_f0 = -np.log(preds_f0[pred_idx])

                    try:
                        importance += np.abs((logprob - logprob_f0)/logprob)
                    except ZeroDivisionError:
                        logprob = 0.0000000001
                        importance += np.abs((logprob - logprob_f0)/logprob)
                        
                    samples_counter += 1

                else:
                    pass

                ttot_f = time.time() - ttot_0
                if n % 10 == 0 and i % 10 == 0:
                    print '    total time for 1 sample: %f' % ttot_f

            try:
                importance = importance/float(samples_counter)
            except ZeroDivisionError:
                importance = 0
                
            self.features_scores[i] = importance

        return self.features_scores

    def get_top_features(self,nb_top_features):

        sorted_scores = sorted(self.features_scores.items(),
                               key=operator.itemgetter(1),
                               reverse=True)
        
        if nb_top_features == 'all':
            return sorted_scores
        else:
            return sorted_scores[:nb_top_features]
        
