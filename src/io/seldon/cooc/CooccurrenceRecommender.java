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

package io.seldon.cooc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.seldon.util.CollectionTools;

public class CooccurrenceRecommender {
	
	int minNumberCooccurrences = 5;
	String client;
	CooccurrencePeer coocPeer;
	ICooccurrenceStore coocStore;
	
	public CooccurrenceRecommender(String client, CooccurrencePeer coocPeer,
			ICooccurrenceStore coocStore) {
		super();
		this.client = client;
		this.coocPeer = coocPeer;
		this.coocStore = coocStore;
	}
	
	private double getJaccardSimilarity(double hitsA,double hitsB,double hitsAB)
	{
		if (hitsAB < this.minNumberCooccurrences || hitsB == 0 || hitsA == 0 || hitsAB == 0)
			return 0;
		else
			return hitsAB/(hitsA+hitsB-hitsAB);
	}
	
	public List<Long> mostPopular(List<Long> items)
	{
		Map<Long,Double> scores = new HashMap<>();
		Map<String,CooccurrenceCount> aaMap = coocPeer.getCountsAA(items, coocStore);
		for(Long item : items)
			scores.put(item, 0.0D);
		for(Map.Entry<String, CooccurrenceCount> e : aaMap.entrySet())
		{
			long[] keys = CooccurrencePeer.getKeyItems(e.getKey());
			scores.put(keys[0], e.getValue().getCount());
		}
		return CollectionTools.sortMapAndLimitToList(scores, items.size(), true);
	}

	public List<Long> sort(long userId,List<Long> recentItemsForUser,List<Long> items)
	{
		List<Long> combinedItems = new ArrayList<>();
		combinedItems.addAll(recentItemsForUser);
		combinedItems.addAll(items);
		Map<String,CooccurrenceCount> aaMap = coocPeer.getCountsAA(combinedItems, coocStore);
		Map<String,CooccurrenceCount> abMap = coocPeer.getCountsAB(recentItemsForUser, items, coocStore);
		Map<Long,Double> scores = new HashMap<>();
		for(Long item : items)
			scores.put(item, 0.0D);
		for(Long item1 : recentItemsForUser)
		{
			CooccurrenceCount c11 = aaMap.get(CooccurrencePeer.getKey(item1, item1));
			for(Long item2 : items)
			{
				CooccurrenceCount c22 = aaMap.get(CooccurrencePeer.getKey(item2, item2));
				CooccurrenceCount c12 = abMap.get(CooccurrencePeer.getKey(item1, item2));
				scores.put(item2, scores.get(item2) + getJaccardSimilarity(c11.getCount(), c22.getCount(), c12.getCount()));
			}
		}
		
		//Remove zero scores
		Map<Long,Double> scoresPos = new HashMap<>();
		for(Map.Entry<Long, Double> e : scores.entrySet())
			if (e.getValue() > 0.0D)
				scoresPos.put(e.getKey(), e.getValue());
		
		return CollectionTools.sortMapAndLimitToList(scoresPos,scoresPos.size(), true);
	}

}
