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

package io.seldon.mahout;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.seldon.trust.impl.Recommendation;

public class FPGrowthRecommender {
	
	FPGrowthStore store;
	
	
	public FPGrowthRecommender(FPGrowthStore store) {
		this.store = store;
	}

	public static class ItemSet
	{
		long support;
		Set<Long> items;
		
		public ItemSet(long support)
		{
			this.support = support;
			items = new HashSet<>();
		}
		
		public ItemSet(int support, Set<Long> items) {
			this.support = support;
			this.items = items;
		}

		public long getSupport() {
			return support;
		}

		public Set<Long> getItems() {
			return items;
		}
		
		public void addItem(Long item)
		{
			items.add(item);
		}
		
	}
	
	public List<Recommendation> recommend(long userId,int type,List<Long> recentItems,int numRecommendations)
	{
		List<Recommendation> recs = new ArrayList<>();
		List<Long> items = getRecommendations(recentItems,numRecommendations);
		for(Long item : items)
			recs.add(new Recommendation(item,type,1D));
		return recs;
	}
	
	public List<Long> sortBySimilarity(List<Long> recentItems,Collection<Long> items)
	{
		if (recentItems == null || recentItems.size() == 0 || items == null || items.size() == 0)
			return new ArrayList<>();
		else
			return store.getOrderedItems(recentItems, items);
	}
	
	public List<Long> getRecommendations(List<Long> transaction,int numResults)
	{
		return store.getSupportedItems(transaction,numResults);
	}

}
