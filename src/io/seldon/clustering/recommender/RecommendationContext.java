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

import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.ItemFilter;
import io.seldon.trust.impl.ItemIncluder;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * Information that the Item Recommendation algorithms require to calculate what they should recommend. The main concept
 * is the 'mode'. Inclusion mode means that the context items are a set of items to recommend from, where as exclusion
 * mode means that they context items are a set of items to exclude from the recommendations.
 *
 * @author firemanphil
 *         Date: 25/11/14
 *         Time: 16:06
 */
public class RecommendationContext {
    private final String lastRecListUUID;

    public enum MODE {
        INCLUSION, EXCLUSION, NONE
    }
    private final MODE mode;
    private final Set<Long> contextItems;
    private final Long currentItem;

    public RecommendationContext(MODE mode, Set<Long> contextItems, Long currentItem, String lastRecListUUID) {
        this.mode = mode;
        this.contextItems = contextItems;
        this.currentItem = currentItem;
        this.lastRecListUUID = lastRecListUUID;
    }

    public String getLastRecListUUID() {
        return lastRecListUUID;
    }

    public MODE getMode() {
        return mode;
    }

    public Set<Long> getContextItems() {
        return contextItems;
    }

    public Long getCurrentItem() {
        return currentItem;
    }

    public static RecommendationContext buildContext(Set<ItemIncluder> inclusionProducers, Set<ItemFilter> itemFilters,
                                                     String client, Long user, String clientUserId,
                                                     Long currentItem, int dimensionId,
                                                     String lastRecListUUID, int itemsPerIncluder, int numRecommendations, CFAlgorithm options){
        Set<Long> contextItems = new HashSet<>();
        if(inclusionProducers == null || inclusionProducers.size() ==0){
            if (itemFilters==null || itemFilters.size() == 0){
                return new RecommendationContext(MODE.NONE, Collections.<Long>emptySet(), currentItem, lastRecListUUID);
            }
            for (ItemFilter filter : itemFilters){
                contextItems.addAll(filter.produceExcludedItems(client, user,clientUserId, currentItem, lastRecListUUID,
                        numRecommendations, options));
            }
            return new RecommendationContext(MODE.EXCLUSION, contextItems, currentItem, lastRecListUUID);
        }

        if(itemFilters == null || itemFilters.size() ==0) {
            for (ItemIncluder producer : inclusionProducers){
                contextItems.addAll(producer.generateIncludedItems(client, dimensionId, itemsPerIncluder));
            }
            return new RecommendationContext(MODE.INCLUSION, contextItems, currentItem, lastRecListUUID);
        }

        Set<Long> included = new HashSet<>();
        Set<Long> excluded = new HashSet<>();
        for (ItemFilter filter : itemFilters){
            excluded.addAll(filter.produceExcludedItems(client, user,clientUserId, currentItem, lastRecListUUID,
                    numRecommendations, options));
        }
        for (ItemIncluder producer : inclusionProducers){
            included.addAll(producer.generateIncludedItems(client, dimensionId, itemsPerIncluder));
        }
        included.removeAll(excluded);

        return new RecommendationContext(MODE.INCLUSION, included, currentItem, lastRecListUUID);
    }


}
