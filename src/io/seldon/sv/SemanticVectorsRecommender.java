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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.clustering.recommender.ItemRecommendationResultSet.ItemRecommendationResult;
import io.seldon.clustering.recommender.MemcachedAssistedAlgorithm;
import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.semvec.LongIdTransform;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.ItemFilter;
import io.seldon.trust.impl.ItemIncluder;

public class SemanticVectorsRecommender extends MemcachedAssistedAlgorithm {
	
	SemanticVectorsManager svManager;

	public SemanticVectorsRecommender(SemanticVectorsManager svManager, List<ItemIncluder> producers, List<ItemFilter> filters)
	{
		this.svManager = svManager;
	}
	
	@Override
    public ItemRecommendationResultSet recommend(CFAlgorithm options,String client, Long user, int dimensionId,
												 int maxRecsCount, RecommendationContext ctxt, List<Long> recentitemInteractions) {
		return recommendWithoutCache(options,client, user, dimensionId, ctxt,maxRecsCount, recentitemInteractions);
	}

	@Override
	public String name() {
		return "semantic_vecs";
	}

	@Override
	public ItemRecommendationResultSet recommendWithoutCache(CFAlgorithm options,String client,
			Long user, int dimension, RecommendationContext ctxt, int maxRecsCount,List<Long> recentItemInteractions) {

		if (recentItemInteractions.size() == 0)
		{
			logger.debug("Can't recommend as no recent item interactions");
			return new ItemRecommendationResultSet(Collections.<ItemRecommendationResult>emptyList());
		}

		SemanticVectorsStore svPeer = svManager.getStore(client, options.getSvPrefix());
		if (svPeer == null)
		{
			logger.debug("Failed to find sv peer for client "+client+" with type "+options.getSvPrefix());
			return new ItemRecommendationResultSet(Collections.<ItemRecommendationResult>emptyList());
		}
		
		List<Long> itemsToScore;
		if(recentItemInteractions.size() > options.getNumRecentActions()) 
		{
			 logger.debug("Limiting recent items for score to size "+options.getNumRecentActions()+" from present "+recentItemInteractions.size());
			itemsToScore = recentItemInteractions.subList(0, options.getNumRecentActions());
		}
		else
			itemsToScore = new ArrayList<Long>(recentItemInteractions);

		
		Map<Long,Double> recommendations;

		if (ctxt.getMode() == RecommendationContext.MODE.INCLUSION)
		{
			// compare recentItemInteractions against.getContextItems() and choose best
			recommendations = svPeer.recommendDocsUsingDocQuery(itemsToScore, new HashSet<Long>(recentItemInteractions),ctxt.getContextItems() , new LongIdTransform(),maxRecsCount,options.isIgnorePerfectSVMatches());
		}
		else 
		{
			//compare recentItemInteactions against all items and choose best using context and exclusions
			Set<Long> itemExclusions = ctxt.getMode() == RecommendationContext.MODE.INCLUSION ? Collections.<Long>emptySet() : ctxt.getContextItems();
			recommendations = svPeer.recommendDocsUsingDocQuery(recentItemInteractions, new LongIdTransform(), maxRecsCount, itemExclusions, null, options.isIgnorePerfectSVMatches());
		}
		List<ItemRecommendationResult> results = new ArrayList<ItemRecommendationResult>();
		for(Map.Entry<Long, Double> e : recommendations.entrySet())
		{
			results.add(new ItemRecommendationResult(e.getKey(), e.getValue().floatValue()));
		}
		return new ItemRecommendationResultSet(results);
	}
}
