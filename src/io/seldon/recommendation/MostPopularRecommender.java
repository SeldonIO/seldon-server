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
import io.seldon.general.jdo.SqlItemPeer;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author firemanphil
 *         Date: 11/12/14
 *         Time: 16:42
 */
@Component
public class MostPopularRecommender implements ItemRecommendationAlgorithm {
	private static final String name = MostPopularRecommender.class.getSimpleName();
    private static Logger logger = Logger.getLogger(MostPopularRecommender.class.getName());

    private ItemStorage itemStorage;

    @Autowired
    public MostPopularRecommender(ItemStorage itemStorage){
        this.itemStorage = itemStorage;
    }

    @Override
    public ItemRecommendationResultSet recommend(String client, Long user, int dimensionId, int maxRecsCount, RecommendationContext ctxt, List<Long> recentItemInteractions) {
        Set<Long> exclusions;
        if(ctxt.getMode() != RecommendationContext.MODE.EXCLUSION){
            logger.warn("Trying to use MostPopularRecommender in an invalid inclusion/exclusion mode, returning empty result set.");
            return new ItemRecommendationResultSet(name);
        } else {
             exclusions = ctxt.getContextItems();
        }

        List<SqlItemPeer.ItemAndScore> itemsToConsider = itemStorage.retrieveMostPopularItemsWithScore(client,maxRecsCount + exclusions.size(),dimensionId);
        List<ItemRecommendationResultSet.ItemRecommendationResult> results = new ArrayList<>();
        for (SqlItemPeer.ItemAndScore itemAndScore : itemsToConsider){
            if(!exclusions.contains(itemAndScore.item))
                results.add(new ItemRecommendationResultSet.ItemRecommendationResult(itemAndScore.item, itemAndScore.score.floatValue()));
        }

        return new ItemRecommendationResultSet(
                itemsToConsider.size() >= maxRecsCount ?
                        new ArrayList<>(results).subList(0,maxRecsCount) :
                        new ArrayList<>(results), name);


    }

    @Override
    public String name() {
        return name;
    }
}
