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

package io.seldon.topics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.clustering.recommender.MemcachedAssistedAlgorithm;
import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.clustering.recommender.ItemRecommendationResultSet.ItemRecommendationResult;
import io.seldon.items.RecentItemsWithTagsManager;
import io.seldon.topics.TopicFeaturesManager.TopicFeaturesStore;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.ItemFilter;
import io.seldon.trust.impl.ItemIncluder;
import io.seldon.trust.impl.jdo.RecommendationUtils;

public class RecentTopicModelRecommender extends MemcachedAssistedAlgorithm {

	TopicFeaturesManager featuresManager;
	RecentItemsWithTagsManager tagsManager;

	public RecentTopicModelRecommender(TopicFeaturesManager featuresManager,RecentItemsWithTagsManager tagsManager,
								 List<ItemIncluder> producers, List<ItemFilter> filters)
	{
		this.featuresManager = featuresManager;
		this.tagsManager = tagsManager;
	}
	
	@Override
    public ItemRecommendationResultSet recommend(CFAlgorithm options,String client, Long user, int dimensionId,
												 int maxRecsCount, RecommendationContext ctxt,List<Long> recentitemInteractions) {
		return recommendWithoutCache(options,client, user, dimensionId, ctxt,maxRecsCount, recentitemInteractions);
	}

	@Override
	public String name() {
		return "recent_topic";
	}

	@Override
	public ItemRecommendationResultSet recommendWithoutCache(CFAlgorithm options,String client,
			Long user, int dimension, RecommendationContext ctxt, int maxRecsCount, List<Long> recentItemInteractions) {
		TopicFeaturesStore store = featuresManager.getClientStore(client);
		if (store == null)
		{
			logger.debug("Failed to find topic features for client "+client);
			return new ItemRecommendationResultSet(Collections.<ItemRecommendationResult>emptyList());
		}
		
		if (ctxt == null || ctxt.getContextItems() == null || ctxt.getContextItems().size() == 0)
		{
			logger.warn("Not items passed in to recommend from. For client "+client);
			return new ItemRecommendationResultSet(Collections.<ItemRecommendationResult>emptyList());
		}
		
		logger.debug("retrive tags for recent items with attr-id "+options.getTagAttrId()+" from table "+options.getTagTable());
		Map<Long,List<String>> itemTags = tagsManager.retrieveRecentItems(client, ctxt.getContextItems(), options.getTagAttrId(),options.getTagTable());
		if (itemTags == null || itemTags.size() == 0)
		{
			logger.debug("Failed to find recent tag items for client "+client);
			return new ItemRecommendationResultSet(Collections.<ItemRecommendationResult>emptyList());
		}
		else
			logger.debug("Got "+itemTags.size()+" recent item tags");
		
		 List<Long> itemsToScore;
		 if(recentItemInteractions.size() > options.getNumRecentActions())
		 {
			 logger.debug("Limiting recent items for score to size "+options.getNumRecentActions()+" from present "+recentItemInteractions.size());
			 itemsToScore = recentItemInteractions.subList(0, options.getNumRecentActions());
		 }
		 else
			 itemsToScore = new ArrayList<>(recentItemInteractions);
		
		//Create user topic weights
		float[] userTopicWeight = createUserTopicsFromRecentItems(itemsToScore, store, itemTags,options.getMinNumTagsForTopicWeights());
		if (userTopicWeight == null)
		{
			logger.debug("Failed to create vector from recent items");
			return new ItemRecommendationResultSet(Collections.<ItemRecommendationResult>emptyList());
		}

		Map<Long,Double> scores = new HashMap<>();
		for(Map.Entry<Long, List<String>> e : itemTags.entrySet())
		{
			if (e.getValue().size() >= options.getMinNumTagsForTopicWeights() && !recentItemInteractions.contains(e.getKey()))
			{
				float[] itemTopicWeight = store.getTopicWeights(e.getKey(), e.getValue());
				Double score = new Double(dot(userTopicWeight,itemTopicWeight));
				logger.debug("Score for "+e.getKey()+"->"+score);
				scores.put(e.getKey(), score);
			}

		}
	
		
		Map<Long,Double> scaledScores = RecommendationUtils.rescaleScoresToOne(scores, maxRecsCount);
		List<ItemRecommendationResult> results = new ArrayList<>();
		for(Map.Entry<Long, Double> e : scaledScores.entrySet())
		{
			results.add(new ItemRecommendationResult(e.getKey(), e.getValue().floatValue()));
		}
		return new ItemRecommendationResultSet(results);
	}
	
	private static float dot(float[] vec1, float[] vec2)
	{
		float sum = 0;
		for (int i = 0; i < vec1.length; i++){
			sum += vec1[i] * vec2[i];
		}
		return sum;
	}

	private float[] createUserTopicsFromRecentItems(List<Long> recentitemInteractions,TopicFeaturesStore store,Map<Long,List<String>> itemTags,int minNumTags)
	{
		float[] scores = new float[store.getNumTopics()];
		if (recentitemInteractions.size()>0)
		{
			float found = 0.0f;
			for(Long item : recentitemInteractions)
			{
				List<String> tags = itemTags.get(item);
				if (tags != null)
				{
					logger.debug("Item "+item+" num tags "+tags.size()+ " limit "+minNumTags);
					if (tags != null && tags.size() >= minNumTags)
					{
						found++;
						float[] scoresItem = store.getTopicWeights(item, tags);
						for(int i=0;i<scores.length;i++)
							scores[i] += scoresItem[i];
					}
				}
			}
			if (found > 0)
			{
				for(int i=0;i<scores.length;i++)
					scores[i] = scores[i] / found;
				return scores;
			}
			else
				return null;
		}
		else return null;

	}
	
	
}
