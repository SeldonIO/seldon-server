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

package io.seldon.sv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;

import io.seldon.semvec.StringTransform;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import pitt.search.semanticvectors.FlagConfig;
import pitt.search.semanticvectors.LuceneUtils;
import pitt.search.semanticvectors.SearchResult;
import pitt.search.semanticvectors.VectorSearcher;
import pitt.search.semanticvectors.VectorStore;
import pitt.search.semanticvectors.VectorStoreRAM;
import pitt.search.semanticvectors.vectors.Vector;
import pitt.search.semanticvectors.vectors.ZeroVectorException;

import io.seldon.clustering.tag.IItemTagCache;
import io.seldon.semvec.LongIdTransform;
import io.seldon.semvec.QueryTransform;
import io.seldon.semvec.SemVectorResult;
import io.seldon.semvec.VectorStorePredictor;
import io.seldon.semvec.VectorStoreRecommender;
import io.seldon.trust.impl.jdo.RecommendationUtils;
import io.seldon.util.CollectionTools;

public class SemanticVectorsStore {
	
	private static Logger logger = Logger.getLogger( SemanticVectorsStore.class.getName() );
	
	private VectorStore termVecReader;
	private VectorStore docVecReader;
	private LuceneUtils luceneUtils = null;
	
	String baseDir;
	String key;
	String termFilename;
	String docFilename;
	Timer reloadTimer; // The timer to be used to reload term/doc databases
	boolean useRamStores = true;
	String[] predictionSuffixes = new String[] {"1","2","3","4","5" };
	FlagConfig flagConfig = FlagConfig.getFlagConfig(null);
	
	public SemanticVectorsStore(VectorStoreRAM termStore,VectorStoreRAM docStore)
	{
		this.termVecReader = termStore;
		this.docVecReader = docStore;
		this.useRamStores = true;
	}
	
	
	
	
	public <T extends Comparable<T>>void searchTermsUsingTermQuery(T termQuery,ArrayList<SemVectorResult<T>> docResult,QueryTransform<T> termTransform,int numResults)
	{
		String query = termTransform.toSV(termQuery);
		LinkedList<SearchResult> results = search(query,termVecReader,termVecReader,numResults);
		for(SearchResult r : results)
		{
			String filename = r.getObjectVector().getObject().toString();
			docResult.add(new SemVectorResult<>(termTransform.fromSV(filename),r.getScore()));
		}
	}
	
	
	public <T extends Comparable<T>,L extends Comparable<L>>void searchDocsUsingTermQuery(L termQuery,ArrayList<SemVectorResult<T>> docResult,QueryTransform<T> docTransform,QueryTransform<L> termTransform,int numResults)
	{
		String query = termTransform.toSV(termQuery); 
		LinkedList<SearchResult> results = search(query,termVecReader,docVecReader,numResults);
		for(SearchResult r : results)
		{
			String filename = r.getObjectVector().getObject().toString();
			docResult.add(new SemVectorResult<>(docTransform.fromSV(filename),r.getScore()));
		}
	}
	
	
	public <T extends Comparable<T>,L extends Comparable<L>>void recommendDocsUsingTermQuery(L termQuery,ArrayList<SemVectorResult<T>> docResult,QueryTransform<T> docTransform,QueryTransform<L> termTransform,int numResults,Set<T> exclusions,Set<T> inclusions,T minDoc)
	{
		String query = termTransform.toSV(termQuery); 
		Set<String> docExclusions = new HashSet<>();
		if (exclusions != null)
			for(T i : exclusions)
				docExclusions.add(docTransform.toSV(i));
		Set<String> docInclusions = new HashSet<>();
		if (inclusions != null)
			for(T i : inclusions)
				docInclusions.add(docTransform.toSV(i));
		LinkedList<SearchResult> results = recommend(query,termVecReader,docVecReader,numResults,docExclusions,docInclusions,docTransform.toSV(minDoc));
		for(SearchResult r : results)
		{
			String filename = r.getObjectVector().getObject().toString();
			docResult.add(new SemVectorResult<>(docTransform.fromSV(filename),r.getScore()));
		}
	}
	
	
	public Double predict(long user,long item)
	{
		VectorStorePredictor predictor = new VectorStorePredictor(""+user,termVecReader,docVecReader,null);
		String prediction = predictor.getPrediction(""+item, predictionSuffixes);
		if (prediction != null)
			return Double.parseDouble(prediction);
		else
			return null;
	}

	
	
	public <T extends Comparable<T>>void recommendDocsUsingDocQuery(T docQuery,ArrayList<SemVectorResult<T>> docResult,QueryTransform<T> docTransform,int numResults,Set<T> exclusions,T minDoc)
	{
		String docName = docTransform.toSV(docQuery);
		Set<String> docExclusions = new HashSet<>();
		for(T i : exclusions)
			docExclusions.add(docTransform.toSV(i));
		LinkedList<SearchResult> results = recommend(docName,docVecReader,docVecReader,numResults,docExclusions,new HashSet<String>(),docTransform.toSV(minDoc));
		for(SearchResult r : results)
		{
			String filename = r.getObjectVector().getObject().toString();
			docResult.add(new SemVectorResult<>(docTransform.fromSV(filename),r.getScore()));
		}
	}
	
	
	public <T extends Comparable<T>>void searchDocsUsingDocQuery(T termQuery,ArrayList<SemVectorResult<T>> docResult,QueryTransform<T> docTransform,int numResults)
	{
		String docName = docTransform.toSV(termQuery);
		LinkedList<SearchResult> results = search(docName,docVecReader,docVecReader,numResults);
		for(SearchResult r : results)
		{
			String filename = r.getObjectVector().getObject().toString();
			docResult.add(new SemVectorResult<>(docTransform.fromSV(filename),r.getScore()));
		}
	}
	
	
	
	private LinkedList<SearchResult> search(String query,VectorStore queryStore,VectorStore searchStore,int numResults)
	{
		 VectorSearcher vecSearcher;
		 LinkedList<SearchResult> results = new LinkedList<>();
		 try 
		 {
			 String[] queryTerms = query.split("\\s+");
			 vecSearcher =
		            new VectorSearcher.VectorSearcherCosine(queryStore,
		                                                    searchStore,
		                                                    luceneUtils,
		                                                    flagConfig,
		                                                    queryTerms);
			 results = vecSearcher.getNearestNeighbors(numResults);
			 
			 
		 } catch (pitt.search.semanticvectors.vectors.ZeroVectorException e) {
			 results = new LinkedList<>();

		}
		 return results;
	}
	
	
	private LinkedList<SearchResult> recommend(String query,VectorStore queryStore,VectorStore searchStore,int numResults,Set<String> exclusions,Set<String> inclusions,String minDoc)
	{
	
		 LinkedList<SearchResult> results = new LinkedList<>();
		 try 
		 {
			 String[] queryTerms = query.split("\\s+");
			 VectorStoreRecommender vecSearcher =
		            new VectorStoreRecommender.VectorStoreRecommenderCosine(queryStore,
		                                                    searchStore,
		                                                    luceneUtils,
		                                                    queryTerms,
		                                                    exclusions,
		                                                    inclusions,
		                                                    minDoc);
			 results = vecSearcher.getNearestNeighbors(numResults);
			 
			 
		 } catch (ZeroVectorException e) {
			 results = new LinkedList<>();
		}
		 finally{}
		 return results;
	}
	
	

	
	public <T extends Comparable<T>> double getSimilarity(T a,T b,QueryTransform<T> docTransform)
	{
		String aDoc = docTransform.toSV(a);
		String bDoc = docTransform.toSV(b);
		Vector vectorA = docVecReader.getVector(aDoc);
		Vector vectorB = docVecReader.getVector(bDoc);
		if (vectorA != null && vectorB != null)
			return vectorA.measureOverlap(vectorB);
		else
			return 0D;
	}
	
	public <T extends Comparable<T>> List<T> sortDocsUsingDocQuery(List<T> recentItems,List<T> sortItems,QueryTransform<T> docTransform)
	{
		return sortDocsUsingDocQuery(recentItems, sortItems, docTransform, new HashSet<T>());
	}
	
	/**
	 * Sort a set of items based on similarity with a list of items
	 * @param <T>
	 * @param recentItems
	 * @param sortItems
	 * @param docTransform
	 * @return
	 */
	public <T extends Comparable<T>> List<T> sortDocsUsingDocQuery(List<T> recentItems,List<T> sortItems,QueryTransform<T> docTransform,Set<T> exclusions)
	{
		//various hardwired algorithms - not yet exposed in settings as this is early stage testing and we
		// may only use the best one
		boolean useRank = false;
		boolean bestScore = false;
		boolean useThreshold = false;
		double threshold = 0.999;
		List<T> result = new ArrayList<>();
		Map<Vector,T> sortVectors = new HashMap<>();
		Map<Vector,Double> scores = new HashMap<>();
		boolean comparisonsMade = false;
		boolean foundItemsToSort = false;
		List<T> alreadySeen = new ArrayList<>();
		List<T> notFound = new ArrayList<>();
		for(T item : sortItems)
		{
			if (!recentItems.contains(item) && !exclusions.contains(item))
			{
				Vector v = docVecReader.getVector(docTransform.toSV(item));
				if (v != null && !v.isZeroVector())
				{
					foundItemsToSort = true;
					sortVectors.put(v,item);
					scores.put(v, 0D);
				}
				else
				{
					notFound.add(item);
					logger.warn("Can't find vector for sort item "+item);
				}
			}
			else
			{
				logger.debug("Not sorting already seen article "+item);
				alreadySeen.add(item);
			}
		}
		if (!foundItemsToSort)
		{
			logger.debug("No sort items so returning empty list");
			return new ArrayList<>();
		}
		for(T recent : recentItems)
		{
			logger.debug("Recent item " + recent);
			String recentDoc = docTransform.toSV(recent);
			Vector vectorRecent = docVecReader.getVector(recentDoc);
			if (vectorRecent != null && !vectorRecent.isZeroVector())
			{
				comparisonsMade = true;
				if (useRank)
				{
					Map<Vector,Double> scoresLocal = new HashMap<>();
					for(Map.Entry<Vector, T> e : sortVectors.entrySet())
						scoresLocal.put(e.getKey(), vectorRecent.measureOverlap(e.getKey()));
					List<Vector> orderedLocal = CollectionTools.sortMapAndLimitToList(scoresLocal, scoresLocal.size());
					double count = 1;
					for(Vector vOrdered : orderedLocal)
					{
						scores.put(vOrdered, scores.get(vOrdered)+count);
						count++;
					}
				}
				else
				{
					for(Map.Entry<Vector, T> e : sortVectors.entrySet())
					{
						double overlap = vectorRecent.measureOverlap(e.getKey());
						double current = scores.get(e.getKey());
						if (!Double.isNaN(overlap))
						{
							logger.debug("Overlap with "+e.getValue()+" is "+overlap);
							if (bestScore) // just store best score
							{
								if (overlap > current)
									scores.put(e.getKey(), overlap);
							}
							else
							{
								if (useThreshold) // only add scores for high threshold matches
								{
									if (current < threshold && overlap > current)
										scores.put(e.getKey(),overlap);
									else if (current > threshold && overlap > threshold)
										scores.put(e.getKey(),overlap+current);
								}
								else // add all scores together good or bad
									scores.put(e.getKey(),overlap+current);
							}
						}
					}
				}
			}
			else
				logger.warn("Can't get vector for recent item "+recent);
		}
		if (comparisonsMade)
		{
			List<Vector> ordered;
			if (useRank)
				ordered = CollectionTools.sortMapAndLimitToList(scores, scores.size(),false);
			else
				ordered = CollectionTools.sortMapAndLimitToList(scores, scores.size());
			for(Vector vOrdered : ordered)
			{
				logger.debug("Item " + sortVectors.get(vOrdered) + " has score " +  scores.get(vOrdered));
				result.add(sortVectors.get(vOrdered));
			}
			for(T seenItem : alreadySeen)
			{
				logger.debug("Adding already seen item "+seenItem+" to end of list");
				result.add(seenItem);
			}
			for(T notFoundItem : notFound)
			{
				logger.debug("Adding not found item "+notFoundItem+" to end of list");
				result.add(notFoundItem);
			}
			return result;
		}
		else
		{
			logger.debug("No comparisons made so returning empty list");
			return new ArrayList<>();
		}
	}
	
	
	
	public <T extends Comparable<T>> Map<T,Double> recommendDocsUsingDocQuery(List<T> recentItems,Set<T> sortItems,QueryTransform<T> docTransform,
																			  int numRecommendations,boolean ignorePerfectMatches)
	{
		List<T> result = new ArrayList<>();
		Map<Vector,T> sortVectors = new HashMap<>();
		Map<Vector,Double> scores = new HashMap<>();
		boolean comparisonsMade = false;
		boolean foundItemsToSort = false;
		List<T> alreadySeen = new ArrayList<>();
		List<T> notFound = new ArrayList<>();
		for(T item : sortItems)
		{
			if (!recentItems.contains(item))
			{
				Vector v = docVecReader.getVector(docTransform.toSV(item));
				if (v != null && !v.isZeroVector())
				{
					foundItemsToSort = true;
					sortVectors.put(v,item);
					scores.put(v, 0D);
				}
				else
				{
					notFound.add(item);
					//logger.warn("Can't find vector for sort item "+item);
				}
			}
			else
			{
				logger.debug("Not sorting already seen article "+item);
				alreadySeen.add(item);
			}
		}
		if (!foundItemsToSort)
		{
			logger.debug("No sort items so returning empty list");
			return new HashMap<>();
		}
		for(T recent : recentItems)
		{
			logger.debug("Recent item " + recent);
			String recentDoc = docTransform.toSV(recent);
			Vector vectorRecent = docVecReader.getVector(recentDoc);
			if (vectorRecent != null && !vectorRecent.isZeroVector())
			{
				comparisonsMade = true;
				for(Map.Entry<Vector, T> e : sortVectors.entrySet())
				{
					double overlap = vectorRecent.measureOverlap(e.getKey());
					double current = scores.get(e.getKey());
					if (!Double.isNaN(overlap))
					{
						//logger.debug("Overlap with "+e.getValue()+" is "+overlap);
						if (ignorePerfectMatches && overlap == 1.0)
							logger.info("Ignoring perfect match between "+recent+" and "+e.getValue()+" overlap "+overlap);
						else
							scores.put(e.getKey(),overlap+current);
					}
				}
			}
			else
				logger.warn("Can't get vector for recent item "+recent);
		}
		if (comparisonsMade)
		{
			Map<T,Double> scoresRes = new HashMap<>();
			for(Map.Entry<Vector, Double> e : scores.entrySet())
				scoresRes.put(sortVectors.get(e.getKey()), e.getValue());
			return RecommendationUtils.rescaleScoresToOne(scoresRes, numRecommendations);
		}
		else
		{
			logger.debug("No comparisons made so returning empty list");
			return new HashMap<>();
		}
	}
	
	
	/**
	 * Recommend a set of documents based on some recent viewed documents
	 * @param <T>
	 * @param recentItems : items to use as comparison
	 * @param docTransform : transform an internal id to SV doc
	 * @param numResults : max number of recommendations to return
	 * @param exclusions : items to exclude from returned recommendations
	 * @return
	 */
	//General recommendations
	public <T extends Comparable<T>> Map<T,Double> recommendDocsUsingDocQuery(List<T> recentItems,QueryTransform<T> docTransform,int numResults,Set<T> exclusions,T minDoc,boolean ignorePerfectMatches)
	{
		Map<T,Double> scores = new HashMap<>();
		
		for(T recent : recentItems)
		{
			ArrayList<SemVectorResult<T>> docResult = new ArrayList<>();
			recommendDocsUsingDocQuery(recent,docResult,docTransform,numResults*10,exclusions,minDoc);
			for(SemVectorResult<T> r : docResult)
			{
				Double score = scores.get(r.result);
				if (ignorePerfectMatches && r.score == 1.0)
					logger.info("Ignoring perfect match between "+recent+" and "+r.result+" overlap "+r.score);
				else
				{
					if (score != null)
						score = score + r.score;
					else
						score = r.score;
					scores.put(r.result, score);
				}

			}
		}

		return RecommendationUtils.rescaleScoresToOne(scores, numResults);
		
	}
	
	
	public Map<Long,Double> recommendDocsFromItemTags(List<Long> items,int tagAttrId,IItemTagCache tagCache,int numResults,Set<Long> exclusions,Set<Long> inclusions,boolean ignorePerfectMatches)
	{
		Map<Long,Double> scores = new HashMap<>();
		Set<String> tags = new HashSet<>();
		for(Long item : items)
			tags.addAll(tagCache.getTags(item, tagAttrId)); 
		String query = StringUtils.join(tags, " ");
		ArrayList<SemVectorResult<Long>> docResult = new ArrayList<>();
		recommendDocsUsingTermQuery(query, docResult, new LongIdTransform(), new StringTransform(), numResults, exclusions, inclusions, 1L);
		int numScores = 0;
		for(SemVectorResult<Long> r : docResult)
		{
			Double score = scores.get(r.result);
			if (ignorePerfectMatches && r.score == 1.0)
				logger.debug("Ignoring perfect match between "+query+" and "+r.result+" overlap "+r.score);
			else
			{
				if (score != null)
					score = score + r.score;
				else
					score = r.score;
				logger.debug("Using tags for recommending docs, Adding doc: "+r.result+" score:"+score);
				scores.put(r.result, score);
				numScores++;
			}
		}
		logger.info("Used tags for recommending docs, num scores found:"+numScores+" from items size : "+items.size()+" and tags: "+query);
		
		return RecommendationUtils.rescaleScoresToOne(scores, numResults);
	}
	
	/**
	 * Find similar users by querying the docstore using a query from the terms passed in
	 * @param <T>
	 * @param terms
	 * @param lUtils : lucene utils
	 * @param numResults : max number of results to return
	 * @param docResult : the result list of return ids T
	 * @param docTransform : the transform from document to return id type T
	 */
	public <T extends Comparable<T>> void findSimilarUsersFromTerms(String[] terms,LuceneUtils lUtils,int numResults,ArrayList<SemVectorResult<T>> docResult,QueryTransform<T> docTransform)
	{
		List<SearchResult> results;
		try 
		{
			VectorSearcher vecSearcher =
		            new VectorSearcher.VectorSearcherCosine(termVecReader,
		                                                    docVecReader,
		                                                    luceneUtils,
		                                                    flagConfig,
		                                                    terms);
			results = vecSearcher.getNearestNeighbors(numResults);
		} 
		catch (pitt.search.semanticvectors.vectors.ZeroVectorException e) {
			results = new LinkedList<>();
		}
		for(SearchResult r : results)
		{
			String filename = r.getObjectVector().getObject().toString();
			
			docResult.add(new SemVectorResult<>(docTransform.fromSV(filename),r.getScore()));
		}
	}
	
	public static void main(String[] args)
	{
		/*
		SemVectorsTweetPeer p = new SemVectorsTweetPeer("/home/rummble/data/twitter/semvectors","termvectors.bin","docvectors.bin");
		p.reloadVectorStoresIfPossible("/home/rummble/data/twitter/semvectors","termvectors2.bin","docvectors2.bin");
		ArrayList<SemVectorResult<Long>> res = new ArrayList<SemVectorResult<Long>>();
		p.searchDocsUsingDocQuery(28442278L,res,new DocumentIdTransform());
		for(SemVectorResult<Long> r : res)
			System.out.println(""+r.getResult()+":"+r.getScore());
			*/
		
		/*
		SemVectorsPeer p = new SemVectorsPeer("/home/rummble/data/twitter/football/hashtags/semvectors","t","termvectors2.bin","docvectors2.bin",true);
		ArrayList<SemVectorResult<Long>> res = new ArrayList<SemVectorResult<Long>>();
		p.searchTermsUsingTermQuery(53056865L, res, new LongIdTransform(),10);
		for(SemVectorResult<Long> r : res)
			System.out.println(""+r.getResult()+":"+r.getScore());
			*/
		
		String a = "docs/0000/1234";
		String b = "docs/0000/1235";
		if (a.compareTo(b) < 0)
			System.out.println("less than");
		else
			System.out.println("greater than");
	}

	public VectorStore getTermVecReader() {
		return termVecReader;
	}

	public VectorStore getDocVecReader() {
		return docVecReader;
	}
	
	
}
