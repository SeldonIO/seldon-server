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
import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.general.ItemStorage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author firemanphil
 *         Date: 22/02/15
 *         Time: 11:50
 */
@Component
public class RecentItemsRecommender implements ItemRecommendationAlgorithm {

	private static final String name = RecentItemsRecommender.class.getName();
    private static Logger logger = Logger.getLogger(RecentItemsRecommender.class.getName());
    private final ItemStorage itemStorage;

    @Autowired
    public RecentItemsRecommender(ItemStorage itemStorage){
        this.itemStorage = itemStorage;
    }

    @Override
    public ItemRecommendationResultSet recommend(String client, Long user, int dimensionId, int maxRecsCount, RecommendationContext ctxt, List<Long> recentItemInteractions) {
        HashMap<Long, Double> recommendations = new HashMap<>();
        Set<Long> exclusions;

        if(ctxt.getMode() == RecommendationContext.MODE.INCLUSION){
            logger.warn("Can't run RecentItemsRecommender in inclusion context mode");
            return new ItemRecommendationResultSet(name);
        } else {
            exclusions = ctxt.getContextItems();
        }
        Collection<Long> recList = itemStorage.retrieveRecentlyAddedItems(client,maxRecsCount+exclusions.size(),dimensionId);
        if (recList.size() > 0)
        {
            double scoreIncr = 1.0/(double)recList.size();
            int count = 0;
            for(Long item : recList)
            {
                if (count >= maxRecsCount)
                    break;
                else if (!exclusions.contains(item))
                    recommendations.put(item, 1.0 - (count++ * scoreIncr));
            }
            List<ItemRecommendationResultSet.ItemRecommendationResult> results = new ArrayList<>();
            for (Map.Entry<Long, Double> entry : recommendations.entrySet()){
                results.add(new ItemRecommendationResultSet.ItemRecommendationResult(entry.getKey(), entry.getValue().floatValue()));
            }
            logger.debug("Recent items algorithm returned "+recommendations.size()+" items");
            return new ItemRecommendationResultSet(results, name);
        }
        else
        {
            logger.warn("No items returned for recent items of dimension " + dimensionId + " for " + client);
        }
        return new ItemRecommendationResultSet(Collections.EMPTY_LIST, name);
    }
    @Override
    public String name() {
        return name;
    }
}
