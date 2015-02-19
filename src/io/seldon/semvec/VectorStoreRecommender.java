/*
 * Seldon -- open source prediction engine
 * =======================================
 *
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 * ********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************************************
 */

package io.seldon.semvec;

import java.util.Enumeration;
import java.util.LinkedList;
import java.util.Set;
import java.util.logging.Logger;

import pitt.search.semanticvectors.CompoundVectorBuilder;
import pitt.search.semanticvectors.FlagConfig;
import pitt.search.semanticvectors.LuceneUtils;
import pitt.search.semanticvectors.ObjectVector;
import pitt.search.semanticvectors.SearchResult;
import pitt.search.semanticvectors.VectorStore;
import pitt.search.semanticvectors.vectors.Vector;
import pitt.search.semanticvectors.vectors.ZeroVectorException;

abstract public class VectorStoreRecommender {
	 private static final Logger logger = Logger.getLogger(VectorStoreRecommender.class.getCanonicalName());
	  
	  private VectorStore queryVecStore;
	  private VectorStore searchVecStore;
	  private LuceneUtils luceneUtils;
	  private Set<String> exclusions;
	  private Set<String> inclusions;
	  private String minDoc;
	  /**
	   * This needs to be filled in for each subclass. It takes an individual
	   * vector and assigns it a relevance score for this VectorSearcher.
	   */
	  public abstract double getScore(Vector testVector);
	  public abstract double getScore(Vector v1,Vector v2);

	  /**
	   * Performs basic initialization; subclasses should normally call super() to use this.
	   * @param queryVecStore Vector store to use for query generation.
	   * @param searchVecStore The vector store to search.
	   * @param luceneUtils LuceneUtils object to use for query weighting. (May be null.)
	   */
	  public VectorStoreRecommender(VectorStore queryVecStore,
	                        VectorStore searchVecStore,
	                        LuceneUtils luceneUtils,
	                        Set<String> exclusions,
	                        Set<String> inclusions,
	                        String minDoc) {
	    this.queryVecStore = queryVecStore;
	    this.searchVecStore = searchVecStore;
	    this.luceneUtils = luceneUtils;
	    this.exclusions = exclusions;
	    this.inclusions = inclusions;
	    this.minDoc = minDoc;
	  }

	  /**
	   * This nearest neighbor search is implemented in the abstract
	   * VectorSearcher class itself: this enables all subclasses to reuse
	   * the search whatever scoring method they implement.  Since query
	   * expressions are built into the VectorSearcher,
	   * getNearestNeighbors no longer takes a query vector as an
	   * argument.
	   * @param numResults the number of results / length of the result list.
	   */
	  public LinkedList<SearchResult> getNearestNeighbors(int numResults) {
	    LinkedList<SearchResult> results = new LinkedList<>();
	    double score, threshold = -1;
	    int duplicatesRemoved = 0;
	    Enumeration<ObjectVector> vecEnum = searchVecStore.getAllVectors();

	    while (vecEnum.hasMoreElements()) {
	    	
	    	ObjectVector testElement = vecEnum.nextElement();
	    	
	    	// ignore excluded items
	    	if (exclusions.contains(testElement.getObject().toString()))
	    		continue;
	    	
	    	//only allow includions if specified
	    	if (inclusions != null && inclusions.size()>0 && !inclusions.contains(testElement.getObject().toString()))
	    		continue;
	    	
	    	// ignore items greater than minDoc id (assume doc ids string ordering is useful)
	    	if (minDoc != null && testElement.getObject().toString().compareTo(minDoc) < 0)
	    		continue;
	    	
	    	
	      // Initialize result list if just starting.
	      if (results.size() == 0) {
	        score = getScore(testElement.getVector());
	        results.add(new SearchResult(score, testElement));
	        continue;
	      }

	      // Test this element.

	      score = getScore(testElement.getVector());

	      // This is a way of using the Lucene Index to get term and
	      // document frequency information to reweight all results. It
	      // seems to be good at moving excessively common terms further
	      // down the results. Note that using this means that scores
	      // returned are no longer just cosine similarities.
	      if (this.luceneUtils != null) {
	        score = score *
	            luceneUtils.getGlobalTermWeightFromString((String) testElement.getObject());
	      }

	      if (score > threshold) 
	      {
	        boolean added = false;
	        boolean duplicate = false;
	        for (int i = 0; i < results.size() && !added && !duplicate; ++i) 
	        {
	        	SearchResult r = results.get(i);
	        	
	        	if (score == r.getScore())
	        	{
	        		ObjectVector rVec = (ObjectVector) r.getObjectVector();
	        		double overlap = getScore(rVec.getVector(),testElement.getVector());
	        		double epsilon = Math.abs(overlap-1);
	        		if (epsilon < 0.000001)
	        		{
	        			duplicatesRemoved++;
	        			duplicate = true;
	        			continue;
	        		}
	        	}
	        	// Add to list if this is right place.
	        	if (score > r.getScore() && added == false) 
	        	{
	        		results.add(i, new SearchResult(score, testElement));
	        		added = true;
	        	}
	        }
	        // Prune list if there are already numResults.
	        if (results.size() > numResults) 
	        {
	          results.removeLast();
	          threshold = results.getLast().getScore();
	        } 
	        else 
	        {
	          if (results.size() < numResults && !added && !duplicate) 
	          {
	            results.add(new SearchResult(score, testElement));
	          }
	        }
	      }
	    }
	    if (duplicatesRemoved > 0)
	    	logger.info("removed "+duplicatesRemoved+" duplicates");
	    return results;
	  }
	  
	  
	  /**
	   * Class for searching a vector store using cosine similarity.
	   * Takes a sum of positive query terms and optionally negates some terms.
	   */
	  static public class VectorStoreRecommenderCosine extends VectorStoreRecommender {
	    Vector queryVector;
	    /**
	     * @param queryVecStore Vector store to use for query generation.
	     * @param searchVecStore The vector store to search.
	     * @param luceneUtils LuceneUtils object to use for query weighting. (May be null.)
	     * @param queryTerms Terms that will be parsed into a query
	     * expression. If the string "NOT" appears, terms after this will be negated.
	     */
	    public VectorStoreRecommenderCosine(VectorStore queryVecStore,
	                                VectorStore searchVecStore,
	                                LuceneUtils luceneUtils,
	                                String[] queryTerms,
	                                Set<String> exclusions,
	                                Set<String> inclusions,
	                                String minDoc)
	        throws ZeroVectorException {
	      super(queryVecStore, searchVecStore, luceneUtils, exclusions,inclusions,minDoc);
	      this.queryVector = CompoundVectorBuilder.getQueryVector(queryVecStore,
	                                                              luceneUtils,
	                                                              FlagConfig.getFlagConfig(null),
	                                                              queryTerms);
	      if (this.queryVector.isZeroVector()) {
	        throw new ZeroVectorException("Query vector is zero ... no results.");
	      }
	    }

	    @Override
	    public double getScore(Vector testVector) {
	      //testVector = VectorUtils.getNormalizedVector(testVector);
	      return this.queryVector.measureOverlap(testVector);
	    }

		@Override
		public double getScore(Vector v1, Vector v2) {
			return v1.measureOverlap(v2);
		}
	  }


}
