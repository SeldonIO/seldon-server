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

import io.seldon.clustering.recommender.*;
import io.seldon.general.Item;
import io.seldon.recommendation.combiner.AlgorithmResultsCombiner;
import io.seldon.similarity.item.ItemSimilarityRecommender;
import io.seldon.sv.SemanticVectorsRecommender;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;

import java.util.*;

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
    private static final Map<String, Class<? extends ItemRecommendationAlgorithm>> oldAlgNamesToNew = new HashMap<>();
    static {
        oldAlgNamesToNew.put("CLUSTER_COUNTS_ITEM_CATEGORY", ItemCategoryClusterCountsRecommender.class);
        oldAlgNamesToNew.put("CLUSTER_COUNTS_DYNAMIC", DynamicClusterCountsRecommender.class);
        oldAlgNamesToNew.put("CLUSTER_COUNTS_GLOBAL", GlobalClusterCountsRecommender.class);

        oldAlgNamesToNew.put("CLUSTER_COUNTS_FOR_ITEM", ItemClusterCountsRecommender.class);
        oldAlgNamesToNew.put("CLUSTER_COUNTS_SIGNIFICANT", ItemSignificantCountsRecommender.class);
        oldAlgNamesToNew.put("SEMANTIC_VECTORS", SemanticVectorsRecommender.class);
        oldAlgNamesToNew.put("SIMILAR_ITEMS", ItemSimilarityRecommender.class);
        oldAlgNamesToNew.put("RECENT_ITEMS", RecentItemsRecommender.class);
        oldAlgNamesToNew.put("MOST_POPULAR", MostPopularRecommender.class);
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
            Map<String, ? extends ItemRecommendationAlgorithm> beans = ctxt.getBeansOfType(oldAlgNamesToNew.get(override));
            if(beans.size()!=1){
                logger.error("Couldn't translate old algorithm name " + override + " into algorithm.");
            }
            ItemRecommendationAlgorithm newAlg = beans.values().iterator().next();
            String name = beans.keySet().iterator().next();
            alternate.add(new AlgorithmStrategy(newAlg, first.includers, first.filters, first.config, name));
        }
        return alternate;
    }

    @Override
    public AlgorithmResultsCombiner getAlgorithmResultsCombiner(String userId, String recTag) {
        return baseStrategy.getAlgorithmResultsCombiner(userId, recTag);
    }

    @Override
    public String getName() {
        return baseStrategy.getName();
    }
}
