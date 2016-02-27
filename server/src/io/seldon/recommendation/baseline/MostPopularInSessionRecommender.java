/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.recommendation.baseline;

import io.seldon.api.Constants;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.service.ItemService;
import io.seldon.clustering.recommender.ItemRecommendationAlgorithm;
import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.recommendation.RecommendationUtils;
import io.seldon.recommendation.baseline.MostPopularInSessionFeaturesManager.DimPopularityStore;
import io.seldon.recommendation.baseline.MostPopularInSessionFeaturesManager.ItemCount;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MostPopularInSessionRecommender implements ItemRecommendationAlgorithm {

	private static Logger logger = Logger.getLogger(MostPopularInSessionRecommender.class.getSimpleName());

	private static final String name = MostPopularInSessionRecommender.class.getSimpleName();
	private static final String ATTRS_PROPERTY_NAME ="io.seldon.algorithm.popular.attrs";
	private static final String DEPTH_PROPERTY_NAME ="io.seldon.algorithm.popular.recent.depth";
	
	MostPopularInSessionFeaturesManager itemsManager;
	ItemService itemService;
	
	@Autowired
	public MostPopularInSessionRecommender(MostPopularInSessionFeaturesManager itemsManager,ItemService itemService) {
		this.itemsManager = itemsManager;
		this.itemService = itemService;
	}
	
	/**
	 * Note this recommender does not respect any dimensions passed in
	 */
	@Override
	public ItemRecommendationResultSet recommend(String client, Long user,
			Set<Integer> dimensions, int maxRecsCount,
			RecommendationContext ctxt, List<Long> recentItemInteractions) {
		RecommendationContext.OptionsHolder options = ctxt.getOptsHolder();

		DimPopularityStore store = itemsManager.getClientStore(client, options);
		if (store == null)
		{
			if (logger.isDebugEnabled())
				logger.debug("Failed to find popular session data for client "+client);
			return new ItemRecommendationResultSet(Collections.<ItemRecommendationResultSet.ItemRecommendationResult>emptyList(), name);
		}
		
		String attrs = options.getStringOption(ATTRS_PROPERTY_NAME);
		int maxDepth = options.getIntegerOption(DEPTH_PROPERTY_NAME);
		ConsumerBean c = new ConsumerBean(client);
		String[] attrNames = attrs.split(",");
		Set<Long> exclusions = Collections.emptySet();
        if (ctxt.getMode() == RecommendationContext.MODE.EXCLUSION) {
        	exclusions = ctxt.getContextItems();
        }
        if (logger.isDebugEnabled())
        {
        	logger.debug("user "+user+" recentItems:"+recentItemInteractions.toString()+" depth:"+maxDepth+" attrs "+attrs);
        }
		Map<Long,Double> scores = new HashMap<>();
		for(int depth=0;depth<maxDepth;depth++)
		{
			if (recentItemInteractions.size() <= depth)
				break;
			long recentItem = recentItemInteractions.get(depth);
			Map<String,Integer> attrDims = itemService.getDimensionIdsForItem(c, recentItem);
			double lowestScore = 1.0;
			if (logger.isDebugEnabled())
				logger.debug("Looking at item "+recentItem+" has attrDim size "+attrDims.size());
			for(String attr : attrNames)
			{
				Integer dim = attrDims.get(attr);
				if (dim != null)
				{
					List<ItemCount> counts = store.getTopItemsForDimension(dim);
					if (counts != null)
					{
						double maxCount = 0;
						double lowScore = 1.0;
						for(ItemCount ic : counts)
						{
							if (!exclusions.contains(ic.item))
							{
								Map<String,Integer> attrDimsCandidate = itemService.getDimensionIdsForItem(c, ic.item);
								if (CollectionUtils.containsAny(dimensions, attrDimsCandidate.values()) || dimensions.contains(Constants.DEFAULT_DIMENSION))
								{
									if (logger.isDebugEnabled())
										logger.debug("Adding item "+ic.item+" from dimension "+attr);
									if (maxCount == 0)
										maxCount = ic.count;
									double normCount = (ic.count/maxCount) * lowestScore; //scale to be a score lower than previous values if any
									if (scores.containsKey(ic.item))
										scores.put(ic.item, scores.get(ic.item)+normCount);
									else
										scores.put(ic.item, normCount);
									lowScore = normCount;
									if (scores.size()>= maxRecsCount)
										break;
								}
								else
								{
									if (logger.isDebugEnabled())
										logger.debug("Ignoring prospective item "+ic.item+" as not in dimensions "+dimensions.toString());
								}
							}
							else
							{
								if (logger.isDebugEnabled())
									logger.debug("Excluding item "+ic.item);
							}
						}	
						lowestScore = lowScore;//update lowest from this loop
					}
					else
					{
						if (logger.isDebugEnabled())
							logger.debug("No counts for dimension "+dim+" attribute name "+attr);
					}
				}
				else
				{
					logger.warn("Failed to find attr "+attr+" for item "+recentItem);
				}
				if (scores.size()>= maxRecsCount)
					break;
			}
			
		}
		Map<Long,Double> scaledScores = RecommendationUtils.rescaleScoresToOne(scores, maxRecsCount);
		List<ItemRecommendationResultSet.ItemRecommendationResult> results = new ArrayList<>();
		for(Map.Entry<Long, Double> e : scaledScores.entrySet())
		{
			results.add(new ItemRecommendationResultSet.ItemRecommendationResult(e.getKey(), e.getValue().floatValue()));
		}
		if (logger.isDebugEnabled())
			logger.debug("Returning "+results.size()+" recommendations");
		return new ItemRecommendationResultSet(results, name);
	}
	
	

	@Override
	public String name() {
		return name;
	}

}
