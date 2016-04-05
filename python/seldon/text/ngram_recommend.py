import sys, getopt, argparse
import dawg
import numpy as np
import math
from seldon import Recommender
import operator
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

def find_ngrams(input_list, n):
  return zip(*[input_list[i:] for i in range(n)])


class NgramModel(Recommender):

    def __init__(self,dawg=None):
        """
        Provide n-gram recommender based on loaded arpa ngram model.
        """
        self.dawg=dawg

    def load_model(self,arpa_file):
        """
        Load an arpa model. Creates internal list for keys, probabilities and backoffs.
        """
        keys = []
        probas = []
        backoffs = []
        idx = 0
        ngram_size = 0
        in_preamble = False
        with open(arpa_file) as f:
            for line in f:
                line = line.rstrip()
                if line == "\\data\\":
                    in_preamble = True
                elif line.endswith("-grams:"):
                    in_preamble = False
                    ngram_size = int(line[1:line.index("-")])
                    logger.info("Loading ngrams of length %d",ngram_size)
                elif line == "\\end\\":
                    pass
                else:
                    if not in_preamble and len(line) > 0:
                        parts = line.split("\t")
                        if  not parts[1] in ['<unk>','<s>','</s>']:
                            proba = float(parts[0])
                            ngram = parts[1]
                            if len(parts) > 2:
                                backoff = float(parts[2])
                            else:
                                backoff = 0
                            data = (unicode(ngram, "utf-8"),idx)
                            keys.append(data)
                            probas.append(proba)
                            backoffs.append(backoff)
                            idx += 1
        self.keys = keys
        self.probas = np.array(probas)
        self.backoffs = np.array(backoffs)

    def create_trie(self):
        """
        Create a DAWG from the keys for fast prefix retrieval
        """
        self.dawg = dawg.IntCompletionDAWG(self.keys)

    def fit(self,arpa_file):
        """
        Load an arpa file and store in internal DAWG
        """
        self.load_model(arpa_file)
        self.create_trie()
        
    def score(self,items,k):
        """
        Find best next item given prefix using backoff probabilities.
        """
        recs = defaultdict(float)
        found = 0
        max_ngram_size = len(items) if len(items)<3 else 3
        for ngram_size in range(1,max_ngram_size+1):
            ngrams = find_ngrams(items,ngram_size)
            for ngram in ngrams:
                prefix = " ".join(map(str, ngram))
                prefix = unicode(prefix+" ",'utf8')
                logger.info("searching for %s",prefix)
                tot_proba = 0
                ngram_found = 0
                for key in self.dawg.iterkeys(prefix):
                    tokens = key.split()
                    if len(tokens) == ngram_size+1:
                        proba = self.probas[self.dawg[key]]
                        proba = math.pow(10,proba)
                        logger.info("%s --> %f",key,proba)
                        tot_proba += proba
                        found += 1
                        ngram_found += 1
                        if not ("<s>" in key or "</s>" in key):
                            recs[key.split()[-1]] += proba
                if ngram_found > 0:
                    proba = self.backoffs[self.dawg[prefix[0:-1]]]
                    proba = math.pow(10,proba)
                    tot_proba += proba
                    logger.info("backoff proba for %s %f ",prefix,proba)
                    logger.info("total proba %f",tot_proba)
        if found > 0:
            recs_sorted = sorted(recs.items(), key=operator.itemgetter(0),reverse=True)
            return recs_sorted[0:k]
        else:
            return []

    def recommend(self,user,ids,recent_interactions,client,limit):
        """
        Recommend items

        Parameters
        ----------

        user : long
           user id
        ids : list(long)
           item ids to score
        recent_interactions : list(long)
           recent items the user has interacted with
        client : str
           name of client to recommend for (business group, company, product..)
        limit : int
           number of recommendations to return


        Returns
        -------
        list of pairs of (item_id,score)
        """
        recommendations = []
        scores = self.score(recent_interactions[::-1],limit)
        for (key,score) in scores:
          ikey = int(key)
          if not ikey in recent_interactions:
            recommendations.append((ikey,score))
        return recommendations

                
if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='test ngram prediction')
    parser.add_argument('--model', help='arpa model', required=True)
    parser.add_argument('--search', help='prefix to search space separated tokens', required=True)
    parser.add_argument('--k', help='number of recommendations', type=int, default=5)

    args = parser.parse_args()
    opts = vars(args)

    ngram_predict = NgramModel()
    ngram_predict.fit(args.model)
    items = args.search.split()
    print ngram_predict.recommend(1,[],items,"test",args.k)
