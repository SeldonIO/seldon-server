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

import io.seldon.api.Constants;
import io.seldon.api.caching.ActionHistoryCache;
import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.ItemFilter;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

/**
 * Filter to exclude items that the user has interacted with
 * @author firemanphil
 *         Date: 05/12/14
 *         Time: 16:04
 */
@Component
public class RecentImpressionsFilter implements ItemFilter {
    private static final String RECENT_ACTIONS_NUM = "io.seldon.algorithm.filter.recentactionstofilter";
    private static Logger logger = Logger.getLogger(RecentImpressionsFilter.class.getName());
    
    
    
    @Override
    public List<Long> produceExcludedItems(String client, Long user, String clientUserId, RecommendationContext.OptionsHolder optsHolder,
                                           Long currentItem,String lastRecListUUID, int numRecommendations) {
        if (user != Constants.ANONYMOUS_USER) // only can get recent actions for non anonymous user
        {
            // get recent actions for user
            ActionHistoryCache ah = new ActionHistoryCache(client);
            int recentActionsNum = optsHolder.getIntegerOption(RECENT_ACTIONS_NUM);
            List<Long> recentActions = ah.getRecentActions(user, recentActionsNum >0 ? recentActionsNum : numRecommendations);
            logger.debug("RecentActions for user with client "+client+" internal user id "+user+" num." + recentActions.size());
            return recentActions;
        }
        return Collections.emptyList();
    }
}
