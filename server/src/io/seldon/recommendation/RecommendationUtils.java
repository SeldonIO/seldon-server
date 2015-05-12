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

package io.seldon.recommendation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.seldon.api.logging.CtrLogger;
import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.util.CollectionTools;

import org.apache.log4j.Logger;

public class RecommendationUtils {

	private static final String REMOVE_IGNORED_RECS_OPTION_NAME = "io.seldon.algorithm.filter.removeignoredrecs";
	private static Logger logger = Logger.getLogger(RecommendationUtils.class.getName());
	
	private static final int MEMCACHE_EXCLUSIONS_EXPIRE_SECS = 1800;
	private static final int RECENT_RECS_EXPIRE_SECS = 1800;
	
	
	public static List<Long> getDiverseRecommendations(int numRecommendationsAsked,List<Long> recs,String client,String clientUserId,int dimension)
	{		
		List<Long> recsFinal = new ArrayList<>(recs.subList(0, Math.min(numRecommendationsAsked,recs.size())));
		String rrkey = MemCacheKeys.getRecentRecsForUser(client, clientUserId, dimension);
		Set<Integer> lastRecs = (Set<Integer>) MemCachePeer.get(rrkey);
		int hashCode = recsFinal.hashCode();
		if (lastRecs != null) // only diversify recs if we have already shown recs previously recently
		{
			if (lastRecs.contains(hashCode))
			{
				logger.debug("Trying to diversity recs for user "+clientUserId+" dimension "+dimension+" client"+client+" #recs "+recs.size());
				List<Long> shuffled = new ArrayList<>(recs);
				Collections.shuffle(shuffled); //shuffle 
				shuffled = shuffled.subList(0, Math.min(numRecommendationsAsked,recs.size())); //limit to size of recs asked for
				recsFinal = new ArrayList<>();
				// add back in original order
				for(Long r : recs)
					if (shuffled.contains(r))
						recsFinal.add(r);
				hashCode = recsFinal.hashCode();
			}
			else
				logger.debug("Will not diversity recs for user "+clientUserId+" dimension "+dimension+" as hashcode "+hashCode+" not in "+ CollectionTools.join(lastRecs, ","));
		}
		else
		{
			logger.debug("Will not diversity recs for user "+clientUserId+" dimension "+dimension+" as lasRecs is null");
		}
		if (lastRecs == null)
			lastRecs = new HashSet<>();
		lastRecs.add(hashCode);
		MemCachePeer.put(rrkey, lastRecs,RECENT_RECS_EXPIRE_SECS);
		return recsFinal;
	}
	

	
	/**
	 * Create a new transient recommendations counter for the user by incrementing the current one.
	 * @param client
	 * @param userId
	 * @param currentUUID
	 * @param recs
	 * @param strat
     *@param recTag @return
	 */
	public static String cacheRecommendationsAndCreateNewUUID(String client, String userId, int dimension,
                                                              String currentUUID, List<Long> recs,
                                                              String algKey, Long currentItemId, int numRecentActions, ClientStrategy strat, String recTag)
	{
		String counterKey = MemCacheKeys.getRecommendationListUserCounter(client, dimension, userId);
		Integer userRecCounter = (Integer) MemCachePeer.get(counterKey);
		if (userRecCounter == null)
			userRecCounter = 0;
		try
		{
			userRecCounter++;
			String recsList = CollectionTools.join(recs, ":");
			String abTestingKey = strat.getName(userId, recTag);
			// TODO ab testing and recTag
//			if (algorithm != null)
//				abTestingKey = algorithm.getAbTestingKey();
			CtrLogger.log(false,client, algKey, -1, userId,""+userRecCounter,currentItemId,numRecentActions,recsList,abTestingKey,recTag);
			MemCachePeer.put(MemCacheKeys.getRecommendationListUUID(client,userId,userRecCounter, recTag),new LastRecommendationBean(algKey, recs),MEMCACHE_EXCLUSIONS_EXPIRE_SECS);
			MemCachePeer.put(counterKey, userRecCounter,MEMCACHE_EXCLUSIONS_EXPIRE_SECS);
		}
		catch(NumberFormatException e)
		{
			logger.error("Can decode user UUID as integer: "+currentUUID);
		}
		return ""+userRecCounter;
	}
	
	public static <T extends Comparable<T>> Map<T,Double> normaliseScores(Map<T,Double> scores,int numRecommendations)
	{
		//limit map to recommendation size
		scores = CollectionTools.sortMapAndLimit(scores, numRecommendations);
		//Normalise counts
		double sum = 0;
		for(Map.Entry<T, Double> e : scores.entrySet())
			sum = sum + e.getValue();
		if (sum > 0)
		{
			for(Map.Entry<T, Double> e : scores.entrySet())
				e.setValue(e.getValue()/sum);
			return scores;
		}
		else
		{
			logger.debug("Zero sum in counts - returning empty score map");
			return new HashMap<>();
		}
	}
	
	public static <T extends Comparable<T>> Map<T,Double> rescaleScoresToOne(Map<T,Double> scores,int numRecommendations)
	{
		//limit map to recommendation size
		scores = CollectionTools.sortMapAndLimit(scores, numRecommendations);
		//Normalise counts
		double max = 0;
		for(Map.Entry<T, Double> e : scores.entrySet())
			if (e.getValue() > max)
				max = e.getValue();
		if (max > 0)
		{
			for(Map.Entry<T, Double> e : scores.entrySet())
				e.setValue(e.getValue()/max);
			return scores;
		}
		else
		{
			logger.debug("Zero sum in counts - returning empty score map");
			return new HashMap<>();
		}
	}

}
