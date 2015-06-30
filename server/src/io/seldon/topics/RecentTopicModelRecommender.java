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

import io.seldon.clustering.recommender.ItemRecommendationAlgorithm;
import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.clustering.recommender.ItemRecommendationResultSet.ItemRecommendationResult;
import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.items.RecentItemsWithTagsManager;
import io.seldon.recommendation.ItemFilter;
import io.seldon.recommendation.ItemIncluder;
import io.seldon.recommendation.RecommendationUtils;
import io.seldon.topics.TopicFeaturesManager.TopicFeaturesStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RecentTopicModelRecommender implements ItemRecommendationAlgorithm {
	private static Logger logger = Logger.getLogger(RecentTopicModelRecommender.class.getName());
	private static final String name = RecentTopicModelRecommender.class.getSimpleName();
	private static final String ATTR_ID_PROPERTY_NAME ="io.seldon.algorithm.tags.attrid";
	private static final String TABLE_PROPERTY_NAME = "io.seldon.algorithm.tags.table";
	private static final String MIN_NUM_WEIGHTS_PROPERTY_NAME = "io.seldon.algorithm.tags.minnumtagsfortopicweights";
	private static final String RECENT_ACTIONS_PROPERTY_NAME = "io.seldon.algorithm.general.numrecentactionstouse";

	TopicFeaturesManager featuresManager;
	RecentItemsWithTagsManager tagsManager;

	@Autowired
	public RecentTopicModelRecommender(TopicFeaturesManager featuresManager,RecentItemsWithTagsManager tagsManager,
								 List<ItemIncluder> producers, List<ItemFilter> filters)
	{
		this.featuresManager = featuresManager;
		this.tagsManager = tagsManager;
	}

	@Override
	public String name() {
		return name;
	}

	@Override
	public ItemRecommendationResultSet recommend(String client,
			Long user, Set<Integer> dimensions,int maxRecsCount, RecommendationContext ctxt,  List<Long> recentItemInteractions) {
		RecommendationContext.OptionsHolder options = ctxt.getOptsHolder();
		Integer	tagAttrId = options.getIntegerOption(ATTR_ID_PROPERTY_NAME);
		String tagTable = options.getStringOption(TABLE_PROPERTY_NAME);
		Integer minNumTagsForWeights = options.getIntegerOption(MIN_NUM_WEIGHTS_PROPERTY_NAME);
		int numRecentActionsToUse = options.getIntegerOption(RECENT_ACTIONS_PROPERTY_NAME);
		TopicFeaturesStore store = featuresManager.getClientStore(client,ctxt);
		if (store == null)
		{
			if (logger.isDebugEnabled())
				logger.debug("Failed to find topic features for client "+client);
			return new ItemRecommendationResultSet(Collections.<ItemRecommendationResult>emptyList(), name);
		}
		
		if (ctxt == null || ctxt.getContextItems() == null || ctxt.getContextItems().size() == 0)
		{
			logger.warn("Not items passed in to recommend from. For client "+client);
			return new ItemRecommendationResultSet(Collections.<ItemRecommendationResult>emptyList(), name);
		}
		
		if (logger.isDebugEnabled())
			logger.debug("retrive tags for recent items with attr-id "+tagAttrId+" from table "+tagTable);
		Map<Long,List<String>> itemTags = tagsManager.retrieveRecentItems(client, ctxt.getContextItems(),tagAttrId,tagTable);
		if (itemTags == null || itemTags.size() == 0)
		{
			if (logger.isDebugEnabled())
				logger.debug("Failed to find recent tag items for client "+client);
			return new ItemRecommendationResultSet(Collections.<ItemRecommendationResult>emptyList(), name);
		}
		else if (logger.isDebugEnabled())
			logger.debug("Got "+itemTags.size()+" recent item tags");
		
		 List<Long> itemsToScore;
		 if(recentItemInteractions.size() > numRecentActionsToUse)
		 {
			 if (logger.isDebugEnabled())
				 logger.debug("Limiting recent items for score to size "+numRecentActionsToUse+" from present "+recentItemInteractions.size());
			 itemsToScore = recentItemInteractions.subList(0, numRecentActionsToUse);
		 }
		 else
			 itemsToScore = new ArrayList<>(recentItemInteractions);
		
		//Create user topic weights
		float[] userTopicWeight = createUserTopicsFromRecentItems(itemsToScore, store, itemTags,minNumTagsForWeights);
		if (userTopicWeight == null)
		{
			logger.debug("Failed to create vector from recent items");
			return new ItemRecommendationResultSet(Collections.<ItemRecommendationResult>emptyList(), name);
		}

		Map<Long,Double> scores = new HashMap<>();
		for(Map.Entry<Long, List<String>> e : itemTags.entrySet())
		{
			if (e.getValue().size() >= minNumTagsForWeights && !recentItemInteractions.contains(e.getKey()))
			{
				float[] itemTopicWeight = store.getTopicWeights(e.getKey(), e.getValue());
				Double score = new Double(dot(userTopicWeight,itemTopicWeight));
				if (logger.isDebugEnabled())
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
		return new ItemRecommendationResultSet(results, name);
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
					if (logger.isDebugEnabled())
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
