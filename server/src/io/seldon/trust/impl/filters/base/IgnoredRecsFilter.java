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

package io.seldon.trust.impl.filters.base;

import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.general.ItemStorage;
import io.seldon.trust.impl.ItemFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Filter to exclude items that the user has been shown and yet ignored.
 * @author firemanphil
 *         Date: 10/12/14
 *         Time: 14:47
 */
@Component
public class IgnoredRecsFilter implements ItemFilter {


    private final ItemStorage itemStorage;

    @Autowired
    public IgnoredRecsFilter(ItemStorage itemStorage){
        this.itemStorage = itemStorage;
    }

    @Override
    public List<Long> produceExcludedItems(String client, Long user, String clientUserId,RecommendationContext.OptionsHolder optsHolder,
                                           Long currentItem, String lastRecListUUID, int numRecommendations) {
        Set<Long> alreadyRecommendedAndViewedItems = itemStorage.retrieveIgnoredItems(client, clientUserId);

        return new ArrayList<>(alreadyRecommendedAndViewedItems);
    }
}
