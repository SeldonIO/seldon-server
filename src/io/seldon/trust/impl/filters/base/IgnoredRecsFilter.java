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

import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.ItemFilter;
import io.seldon.trust.impl.jdo.RecommendationUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Filter to exclude items that the user has been shown and yet ignored.
 * @author firemanphil
 *         Date: 10/12/14
 *         Time: 14:47
 */
@Component
public class IgnoredRecsFilter implements ItemFilter {
    @Override
    public List<Long> produceExcludedItems(String client, Long user, String clientUserId,
                                           Long currentItem, String lastRecListUUID, int numRecommendations,
                                           CFAlgorithm options) {
        return new ArrayList<>(RecommendationUtils.getExclusions(false, client, clientUserId, currentItem, lastRecListUUID, options, 0));
    }
}
