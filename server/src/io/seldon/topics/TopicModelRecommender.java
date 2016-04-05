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
import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.items.RecentItemsWithTagsManager;
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
public class TopicModelRecommender implements ItemRecommendationAlgorithm {
	private static Logger logger = Logger.getLogger(TopicModelRecommender.class.getSimpleName());
	private static final String name = TopicModelRecommender.class.getSimpleName();
	private static final String ATTR_ID_PROPERTY_NAME ="io.seldon.algorithm.tags.attrid";
	private static final String TABLE_PROPERTY_NAME = "io.seldon.algorithm.tags.table";
	private static final String MIN_NUM_WEIGHTS_PROPERTY_NAME = "io.seldon.algorithm.tags.minnumtagsfortopicweights";


	TopicFeaturesManager featuresManager;
	RecentItemsWithTagsManager tagsManager;

	@Autowired
	public TopicModelRecommender(TopicFeaturesManager featuresManager,RecentItemsWithTagsManager tagsManager)
	{
		this.featuresManager = featuresManager;
		this.tagsManager = tagsManager;
	}

	@Override
	public ItemRecommendationResultSet recommend(String client,
			Long user, Set<Integer> dimensions, int maxRecsCount, RecommendationContext ctxt, List<Long> recentitemInteractions) {
		RecommendationContext.OptionsHolder options = ctxt.getOptsHolder();
		Integer	tagAttrId = options.getIntegerOption(ATTR_ID_PROPERTY_NAME);
		String tagTable = options.getStringOption(TABLE_PROPERTY_NAME);
		Integer minNumTagsForWeights = options.getIntegerOption(MIN_NUM_WEIGHTS_PROPERTY_NAME);
		TopicFeaturesStore store = featuresManager.getClientStore(client,options);
		if (store == null)
		{
			if (logger.isDebugEnabled())
				logger.debug("Failed to find topic features for client "+client);
			return new ItemRecommendationResultSet(Collections.<ItemRecommendationResultSet.ItemRecommendationResult>emptyList(), name);
		}
		
		if (ctxt == null || ctxt.getContextItems() == null || ctxt.getContextItems().size() == 0)
		{
			logger.warn("Not items passed in to recommend from. For client "+client);
			return new ItemRecommendationResultSet(Collections.<ItemRecommendationResultSet.ItemRecommendationResult>emptyList(), name);
		}
		
		Map<Long,List<String>> itemTags = tagsManager.retrieveRecentItems(client, ctxt.getContextItems(), tagAttrId, tagTable);
		if (itemTags == null || itemTags.size() == 0)
		{
			if (logger.isDebugEnabled())
				logger.debug("Failed to find recent tag items for client "+client);
			return new ItemRecommendationResultSet(Collections.<ItemRecommendationResultSet.ItemRecommendationResult>emptyList(), name);
		}
		else if (logger.isDebugEnabled())
			logger.debug("Got "+itemTags.size()+" recent item tags");
		float[] userTopicWeight = store.getUserWeightVector(user);
		if (userTopicWeight == null)
		{
			return new ItemRecommendationResultSet(Collections.<ItemRecommendationResultSet.ItemRecommendationResult>emptyList(), name);
		}

		Map<Long,Double> scores = new HashMap<>();
		for(Map.Entry<Long, List<String>> e : itemTags.entrySet()) // for all items
		{
			if (e.getValue().size() >= minNumTagsForWeights && !recentitemInteractions.contains(e.getKey()))
			{
				float[] itemTopicWeight = store.getTopicWeights(e.getKey(), e.getValue());
				Double score = new Double(dot(userTopicWeight,itemTopicWeight));
				if (logger.isDebugEnabled())
					logger.debug("Score for "+e.getKey()+"->"+score);
				scores.put(e.getKey(), score);
			}
		}
	
		
		Map<Long,Double> scaledScores = RecommendationUtils.rescaleScoresToOne(scores, maxRecsCount);
		List<ItemRecommendationResultSet.ItemRecommendationResult> results = new ArrayList<>();
		for(Map.Entry<Long, Double> e : scaledScores.entrySet())
		{
			results.add(new ItemRecommendationResultSet.ItemRecommendationResult(e.getKey(), e.getValue().floatValue()));
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

	@Override
	public String name() {
		return name;
	}
}
