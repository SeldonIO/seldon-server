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

package io.seldon.recommendation.baseline;

import java.util.Map;
import java.util.Set;

import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.recommendation.RecommendationUtils;

public class BaselineRecommender {

	final static int CACHE_EXPIRE_SECS = 3600;
	final static int MIN_RECS = 50;
	String client;
	IBaselineRecommenderUtils utils;
	
	
	public BaselineRecommender(String client,IBaselineRecommenderUtils utils)
	{
		this.client = client;
		this.utils = utils;
	}
	
	public Map<Long,Double> mostPopularRecommendations(Set<Long> exclusions,int dimension,int numRecommendations)
	{
		String mkey = MemCacheKeys.getMostPopularItems(client, dimension);
		Map<Long,Double> res = (Map<Long,Double>) MemCachePeer.get(mkey);
		if (res == null)
		{
			res = utils.getPopularItems(dimension, numRecommendations+MIN_RECS);
			MemCachePeer.put(mkey, res,CACHE_EXPIRE_SECS);
		}
		// remove excluded items
		if (exclusions != null)
			for (Long e : exclusions)
				res.remove(e);
		return RecommendationUtils.rescaleScoresToOne(res, numRecommendations);
	}

	public Map<Long,Double> reorderRecommendationsByPopularity(Map<Long,Double> recs)
	{
            String mkey = MemCacheKeys.getMostPopularItems(client);
            Map<Long,Double> res = (Map<Long,Double>) MemCachePeer.get(mkey);
            if (res == null)
            {
                res = utils.getAllItemPopularity();
                MemCachePeer.put(mkey, res, CACHE_EXPIRE_SECS);
            }
            // reorder
            for(Long k : recs.keySet())
            {
                if (res.containsKey(k))
                    recs.put(k, res.get(k));
                else
                    recs.put(k, 0D);
            }
            return recs;
	}
	
}
