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

package io.seldon.clustering.recommender;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import io.seldon.util.CollectionTools;

public class GlobalWeightedMostPopularUtils {

	private static Logger logger = Logger.getLogger(GlobalWeightedMostPopularUtils.class.getName());
	
	MemoryWeightedClusterCountMap counts;
	long time;

	public GlobalWeightedMostPopularUtils(MemoryWeightedClusterCountMap counts,long time) {
		super();
		this.counts = counts;
	}
	
	private Map<Long,Float> rank(List<Long> items)
	{
		Map<Long,Float> rankSorted = new HashMap<Long,Float>();
		float r = 1;
		for(Long item : items)
			rankSorted.put(item, r++);
		return rankSorted;
	}
	
	public List<Long> merge(List<Long> items,float weight)
	{
		List<Long> sorted = sort(items);
		Map<Long,Float> ranked = rank(sorted);
		float r = 1;
		for(Long item : items)
		{
			ranked.put(item, (r * weight) + (ranked.get(item) * (1-weight)));
			r++;
		}
		return CollectionTools.sortMapAndLimitToList(ranked, ranked.size(), false);
	}
	
	public List<Long> sort(List<Long> items)
	{
		Map<Long,Double> scores = new HashMap<Long,Double>();
		for(Long item : items)
		{
			double score = counts.getCount(item, time);
			logger.debug("WeightedMostPopular item:"+item+" score:"+score);
			scores.put(item, score);
		}
		return CollectionTools.sortMapAndLimitToList(scores, scores.size());
	}
	
}
