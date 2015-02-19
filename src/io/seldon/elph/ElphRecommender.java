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

package io.seldon.elph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.util.CollectionTools;
import org.apache.log4j.Logger;

public class ElphRecommender {

	private static Logger logger = Logger.getLogger(ElphRecommender.class.getName());
	
	final static int MEMCACHE_EXPIRE_SEC = 300;
	String client;
	IElphPredictionPeer predictionPeer;

	public ElphRecommender(String client,IElphPredictionPeer predictionPeer) {
		super();
		this.client = client;
		this.predictionPeer = predictionPeer;
	}
	
	public static ArrayList<Long> getSortedUniqueItems(List<Long> recentItems)
	{
		Set<Long> set = new HashSet<>(recentItems);
		ArrayList<Long> sorted = new ArrayList<>(set);
		Collections.sort(sorted);
		return sorted;
	}
	
	public Map<Long,Double> recommend(List<Long> recentItems,Set<Long> exclusions,int dimension)
	{
		ArrayList<Long> sortedUnique = getSortedUniqueItems(recentItems);
		String mkey = MemCacheKeys.getElphPrediction(client, sortedUnique);
		Map<Long,Double> scores = (Map<Long,Double>) MemCachePeer.get(mkey);
		if (scores == null)
		{
			scores = predictionPeer.getPredictions(sortedUnique, dimension);
			if (scores != null)
				MemCachePeer.put(mkey, new HashMap<>(scores), MEMCACHE_EXPIRE_SEC);
		}
		else
			logger.info("Got recommendations from memcache for recent items "+ CollectionTools.join(recentItems, ","));
		//remove exclusions
		if (scores != null && scores.size() > 0)
			for(Long itemId : exclusions)
				scores.remove(itemId);
		return scores;
		
	}

}
