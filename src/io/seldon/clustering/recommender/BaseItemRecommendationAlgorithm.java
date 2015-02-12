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

import io.seldon.trust.impl.ItemFilter;
import io.seldon.trust.impl.ItemIncluder;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author firemanphil
 *         Date: 19/11/14
 *         Time: 13:40
 */
public abstract class BaseItemRecommendationAlgorithm implements ItemRecommendationAlgorithm
{


    private final List<ItemIncluder> inclusionProducers;
    private final List<ItemFilter> itemFilters;

    public BaseItemRecommendationAlgorithm(List<ItemIncluder> producers, List<ItemFilter> filters){
        this.inclusionProducers = producers;
        this.itemFilters = filters;
    }

    protected RecommendationContext retrieveContext(String client, int dimensionId, int itemsPerIncluder){

        Set<Long> contextItems = new HashSet<Long>();
        if(inclusionProducers == null || inclusionProducers.size() ==0){
            if (itemFilters==null || itemFilters.size() == 0){
                return new RecommendationContext(RecommendationContext.MODE.NONE, Collections.<Long>emptySet());
            }
            for (ItemFilter filter : itemFilters){
                contextItems.addAll(filter.produceExcludedItems(client));
            }
            return new RecommendationContext(RecommendationContext.MODE.EXCLUSION, contextItems);
        }

        if(itemFilters == null || itemFilters.size() ==0) {
            for (ItemIncluder producer : inclusionProducers){
                contextItems.addAll(producer.generateIncludedItems(client, dimensionId, itemsPerIncluder));
            }
            return new RecommendationContext(RecommendationContext.MODE.INCLUSION, contextItems);
        }

        Set<Long> included = new HashSet<Long>();
        Set<Long> excluded = new HashSet<Long>();
        for (ItemFilter filter : itemFilters){
            excluded.addAll(filter.produceExcludedItems(client));
        }
        for (ItemIncluder producer : inclusionProducers){
            included.addAll(producer.generateIncludedItems(client, dimensionId, itemsPerIncluder));
        }
        included.removeAll(excluded);

        return new RecommendationContext(RecommendationContext.MODE.INCLUSION, included);
    }


}
