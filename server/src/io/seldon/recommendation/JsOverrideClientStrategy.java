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

package io.seldon.recommendation;

import io.seldon.clustering.recommender.ItemRecommendationAlgorithm;
import io.seldon.recommendation.combiner.AlgorithmResultsCombiner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;

/**
 * Strategy using algorithms passed in a JS call.
 *
 * @author firemanphil
 *         Date: 23/03/15
 *         Time: 13:58
 */
@Deprecated
public class JsOverrideClientStrategy implements ClientStrategy {
    private static Logger logger = Logger.getLogger(JsOverrideClientStrategy.class.getName());
    private static final Map<String, String> oldAlgNamesToNew = new HashMap<>();
    static {
        oldAlgNamesToNew.put("CLUSTER_COUNTS_ITEM_CATEGORY", "itemCategoryClusterCountsRecommender");
        oldAlgNamesToNew.put("CLUSTER_COUNTS_DYNAMIC", "dynamicClusterCountsRecommender");
        oldAlgNamesToNew.put("CLUSTER_COUNTS_GLOBAL", "globalClusterCountsRecommender");

        oldAlgNamesToNew.put("CLUSTER_COUNTS_FOR_ITEM", "itemClusterCountsRecommender");
        oldAlgNamesToNew.put("CLUSTER_COUNTS_SIGNIFICANT", "itemSignificantCountsRecommender");
        oldAlgNamesToNew.put("SEMANTIC_VECTORS", "semanticVectorsRecommender");
        oldAlgNamesToNew.put("SIMILAR_ITEMS", "itemSimilarityRecommender");
        oldAlgNamesToNew.put("RECENT_ITEMS", "recentItemsRecommender");
        oldAlgNamesToNew.put("MOST_POPULAR", "mostPopularRecommender");
        
        oldAlgNamesToNew.put("MATRIX_FACTOR", "mfRecommender");
        oldAlgNamesToNew.put("RECENT_MATRIX_FACTOR", "recentMfRecommender");
        oldAlgNamesToNew.put("RECENT_SIMILAR_ITEMS", "itemSimilarityRecommender");
        oldAlgNamesToNew.put("WORD2VEC", "word2vecRecommender");
        oldAlgNamesToNew.put("TOPIC_MODEL", "topicModelRecommender");
        oldAlgNamesToNew.put("RECENT_TOPIC_MODEL", "recentTopicModelRecommender");
    }
    private final ClientStrategy baseStrategy;
    private final Collection<String> overrideAlgs;
    private final ApplicationContext ctxt;

    public JsOverrideClientStrategy(ClientStrategy baseStrategy, Collection<String> overrideAlgs, ApplicationContext ctxt) {
        this.baseStrategy = baseStrategy;
        this.overrideAlgs = overrideAlgs;
        this.ctxt = ctxt;
    }

    @Override
    public Double getDiversityLevel(String userId, String recTag) {
        return baseStrategy.getDiversityLevel(userId, recTag);
    }

    @Override
    public List<AlgorithmStrategy> getAlgorithms(String userId, String recTag) {
        List<AlgorithmStrategy> baseAlgStrats = baseStrategy.getAlgorithms(userId, recTag);
        List<AlgorithmStrategy> alternate = new ArrayList<>();
        AlgorithmStrategy first = baseAlgStrats.get(0);
        for(String override : overrideAlgs){
            String name = oldAlgNamesToNew.get(override);
            if (name == null) {
                logger.error("Algorithm name " + override + " Not Found!");
                continue;
            }
            Object bean = ctxt.getBean(oldAlgNamesToNew.get(override));
            if (bean != null) {
                ItemRecommendationAlgorithm newAlg= (ItemRecommendationAlgorithm) bean;
                alternate.add(new AlgorithmStrategy(newAlg, first.includers, first.filters, first.config, name));
            } else {
                logger.error("Couldn't translate old algorithm name " + override + " into algorithm.");
            }
        }
        return alternate;
    }

    @Override
    public AlgorithmResultsCombiner getAlgorithmResultsCombiner(String userId, String recTag) {
        return baseStrategy.getAlgorithmResultsCombiner(userId, recTag);
    }

    @Override
    public String getName(String userId, String recTag) {
        return baseStrategy.getName(userId, recTag);
    }

	@Override
	public Map<Integer, Double> getActionsWeights(String userId, String recTag) {
		return baseStrategy.getActionsWeights(userId, recTag);
	}
}
