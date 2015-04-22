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

import java.util.Map;
import java.util.Set;

/**
 *
 * The strategy for applying an algorithm in the context of item recommendation.
 *
 * @author firemanphil
 *         Date: 19/02/15
 *         Time: 15:05
 */
public class AlgorithmStrategy {

    /**
     * The algorithm to use to create recs.
     */
    public final ItemRecommendationAlgorithm algorithm;
    /**
     * The ItemIncluders to use to provide the set of items to feed into the algorithm.
     */
    public final Set<ItemIncluder> includers;

    /**
     * The ItemFilters to use to exclude items from being recommended.
     */
    public final Set<ItemFilter> filters;

    public final Map<String, String> config;

    /**
     * Textual name for this strategy.
     */
    public final String name;

    public AlgorithmStrategy(ItemRecommendationAlgorithm algorithm, Set<ItemIncluder> includers, Set<ItemFilter> filters, Map<String,String> config, String name) {
        this.algorithm = algorithm;
        this.includers = includers;
        this.filters = filters;
        this.config = config;
        this.name = name;
    }
}
