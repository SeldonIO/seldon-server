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

package io.seldon.similarity.dbpedia;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.trust.impl.SharingRecommendation;
import io.seldon.util.CollectionTools;
import org.apache.log4j.Logger;

import io.seldon.trust.impl.SearchResult;
import io.seldon.trust.impl.jdo.RecommendationUtils;

public class WebSimilaritySimplePeer {

	private static Logger logger = Logger.getLogger(WebSimilaritySimplePeer.class.getName());
	
	private static final int EXPIRE_FRIENDS_RECS_SECS = 60*60*12;
	private static final int EXPIRE_SORT_RECS_SECS = 60*60*12;
	private static final int EXPIRE_SIMILAR_USERS_SECS = 60*60*12;


	String client;
	WebSimilaritySimpleStore store;
	boolean allowFullSocialPredictionSearch = false;
	String linkType;
	
	public WebSimilaritySimplePeer(String client,WebSimilaritySimpleStore store)
	{
		this.client = client;
		this.store = store;
	}
	
	public WebSimilaritySimplePeer(String client,WebSimilaritySimpleStore store,String linkType,boolean allowFullSocialPredictionSearch)
	{
		this.client = client;
		this.store = store;
		this.linkType = linkType;
		this.allowFullSocialPredictionSearch = allowFullSocialPredictionSearch;
	}
	
	private List<SharingRecommendation> getSharingRecommendations(List<Long> users,List<Long> items)
	{
		return store.getSharingRecommendations(users, items);
	}
	
	public List<SharingRecommendation> getSharingRecommendationsForFriends(long userId,List<String> keywords)
	{
		List<SharingRecommendation> res = null;
		String memcacheKey = MemCacheKeys.getSharingRecommendationForKeywords(client, userId, keywords);
		res = (List<SharingRecommendation>) MemCachePeer.get(memcacheKey);
		if (res == null)
		{
			logger.info("Calling db to get sharing recommendations for user "+userId);
			long t1 = System.currentTimeMillis();
			res = store.getSharingRecommendationsForFriends(userId, keywords);
			long t2 = System.currentTimeMillis();
			if (res != null)
			{
				logger.info("Got "+res.size()+" results in "+(t2-t1)+" for user "+userId);
				MemCachePeer.put(memcacheKey, res,EXPIRE_FRIENDS_RECS_SECS);
			}
		}
		if ( res != null)
			return res;
		else
			return new ArrayList<SharingRecommendation>();
	}
	
	public  List<SharingRecommendation> getSharingRecommendationsForFriends(long userId,long itemId)
	{
		List<SharingRecommendation> res = null;
		String memcacheKey = MemCacheKeys.getSharingRecommendationsForItemSetKey(client, userId);
		Map<Long,List<SharingRecommendation>> map = (Map<Long,List<SharingRecommendation>>) MemCachePeer.get(memcacheKey);
		if (map != null)
		{
			res = map.get(itemId);
			if (res != null)
				logger.info("Found from memcached map for user "+userId+" for itemId"+itemId+" with size "+res.size());
			else
				logger.info("Failed to find itemId "+itemId+" for user "+userId+" in mapped memcache results");
		}
		else
			logger.info("No map memcache results for user "+userId);
		if (res == null)
		{
			memcacheKey = MemCacheKeys.getSharingRecommendationKey(client, userId, itemId, linkType);
			res = (List<SharingRecommendation>) MemCachePeer.get(memcacheKey);
			if (res == null)
			{
				//previous usage:
				//List<Long> items = new ArrayList<Long>();
				//items.add(itemId);
				//res = store.getSharingRecommendationsForFriends(userId, linkType, items);
				//FIXME linktype ignored
				
				logger.info("Calling store to get searched results for item "+itemId+" for user "+userId);
				res = store.getSharingRecommendationsForFriends(userId, itemId);
				if (res == null || res.size() == 0)
				{
					if (allowFullSocialPredictionSearch)
					{
						if (!store.hasBeenSearched(itemId))
						{
							logger.info("Doing a full sharing recommendation search as item "+itemId+" for user "+userId+" as has not been searched");
							res = store.getSharingRecommendationsForFriendsFull(userId, itemId);
						}
						else
							logger.info("Already searched for "+itemId+" so no full search done for user "+userId);
					}
					else
						logger.info("Not doing full sharing recommendation search as settings is false");
				}
				else
					MemCachePeer.put(memcacheKey, res,EXPIRE_FRIENDS_RECS_SECS);
				
			}
			else
				logger.info("Returning sharing results from memcache for user "+userId+" for item "+itemId+" with size "+res.size());
		}
		return res;
	}
	
	public List<Long> sort(long userId,List<Long> items)
	{
		String memcacheKey = MemCacheKeys.getSharingRecommendationKey(client, userId, items);
		List<Long> res = (List<Long>) MemCachePeer.get(memcacheKey);
		if (res == null)
		{
			List<Long> users = new ArrayList<Long>();
			users.add(userId);
			List<SharingRecommendation> shares = store.getSharingRecommendations(users, items);
			res = new ArrayList<Long>();
			Set<Long> found = new HashSet<Long>();
			if (shares != null && shares.size() > 0)
			{
				for(SharingRecommendation s : shares)
				{
					logger.info("Adding item "+s.getItemId()+" for user "+s.getUserId()+" with reasons "+ CollectionTools.join(s.getReasons(), ","));
					res.add(s.getItemId());
					found.add(s.getItemId());
				}
			}
			MemCachePeer.put(memcacheKey, res,EXPIRE_SORT_RECS_SECS);
		}
		return res;
	}
	
	
	private Map<Long,Float> rank(List<Long> items)
	{
		Map<Long,Float> rankSorted = new HashMap<Long,Float>();
		float r = 1;
		for(Long item : items)
			rankSorted.put(item, r++);
		return rankSorted;
	}
	
	public List<Long> merge(long userId, List<Long> items,float weight)
	{
		List<Long> sorted = sort(userId,items);
		if (sorted.size() > 0)
		{
			Map<Long,Float> ranked = rank(sorted);
			float r = 1;
			for(Long item : items)
			{
				Float sortedRank = ranked.get(item);
				if (sortedRank == null)
					sortedRank = new Float(sorted.size()+1);
				ranked.put(item, (r * weight) + (sortedRank * (1-weight)));
				r++;
			}
			return CollectionTools.sortMapAndLimitToList(ranked, ranked.size(), false);
		}
		else
			return items;
	}
	
	
	public Map<Long,Double> getRecommendedItems(long userId,int dimension,List<Long> items,Set<Long> exclusions,int cacheTimeSecs,int numRecommendations)
	{
		long hashCode = items.hashCode();
		String key = MemCacheKeys.getSocialPredictRecommendedItems(client, userId, hashCode);
		Map<Long,Double> scores = (Map<Long,Double>) MemCachePeer.get(key);
		if (scores == null)
		{
			List<Long> itemsToSearch = new ArrayList<Long>(items);
			itemsToSearch.removeAll(exclusions);
			scores = store.getSocialPredictionRecommendations(userId, numRecommendations);
			MemCachePeer.put(key, scores, cacheTimeSecs);
		}
		else
		{
			//remove excluded items as Memcache version may not be up to date
			for(Iterator<Map.Entry<Long, Double>> it = scores.entrySet().iterator(); it.hasNext(); )
			{
			      Map.Entry<Long, Double> entry = it.next();
			      if(exclusions.contains(entry.getKey())) 
			      {
			        it.remove();
			      }
			}
		}
		
		if (scores != null)
		{
			return RecommendationUtils.rescaleScoresToOne(scores, numRecommendations);
		}
		else
		{
			logger.warn("Null recommendation list for social prediction for user "+userId+" for client "+client);
			return new HashMap<Long,Double>();
		}
		
	}
	
	public List<SearchResult> getSimilarUsers(long userId,int limit,int similarityMetric, int interactionFilterType )
	{
		String key = MemCacheKeys.getSimilarUsers(client, userId, similarityMetric, interactionFilterType);
		
		Map<Long,Double> similarUsers = (Map<Long,Double>) MemCachePeer.get(key); 
		if (similarUsers == null)
		{
			similarUsers = store.getSimilarUsers(userId, similarityMetric, interactionFilterType);
			MemCachePeer.put(key, similarUsers, EXPIRE_SIMILAR_USERS_SECS);
			logger.info("Got similar users for "+userId+" client "+client+" from db  of size "+similarUsers.size()+" with metric type "+similarityMetric);
		}
		else
			logger.info("Got similar users for "+userId+" client "+client+" from memcache  of size "+similarUsers.size()+" with metric type "+similarityMetric);
		
		List<Long> sorted = CollectionTools.sortMapAndLimitToList(similarUsers, limit, true);
		List<SearchResult> res = new ArrayList<SearchResult>();
		for(Long u : sorted)
		{
			res.add(new SearchResult(u, similarUsers.get(u)));
		}
		return res;
	}

    public void updateUserSimilarityCache(long user1, long user2, int similarityMetric, int filterType) {
        String key = MemCacheKeys.getSimilarUsers(client, user1, similarityMetric, filterType);
        Map<Long,Double> similarUsers = (Map<Long,Double>) MemCachePeer.get(key); 
        if(similarUsers!=null){
            logger.info("Removing user '"+user2+"' from user similarity cache for user '"+user1+
                    "', metric "+similarityMetric +" and filter type" +filterType + " due to an interaction.");
            similarUsers.remove(user2);
            MemCachePeer.put(key, similarUsers);
        }
    }
	
}
