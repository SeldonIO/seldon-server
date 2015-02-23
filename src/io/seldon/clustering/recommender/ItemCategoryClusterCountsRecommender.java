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

import io.seldon.clustering.recommender.jdo.JdoCountRecommenderUtils;
import io.seldon.trust.impl.CFAlgorithm;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * @author firemanphil
 *         Date: 10/12/14
 *         Time: 17:00
 */
@Component
public class ItemCategoryClusterCountsRecommender extends BaseItemCategoryRecommender implements ItemRecommendationAlgorithm {

    private static Logger logger = Logger.getLogger(ItemCategoryClusterCountsRecommender.class.getName());
    @Override
    public ItemRecommendationResultSet recommend(CFAlgorithm options, String client, Long user, int dimensionId, int maxRecsCount, RecommendationContext ctxt, List<Long> recentItemInteractions) {
        if (ctxt.getCurrentItem() != null)
        {
            Set<Long> exclusions = Collections.emptySet();
            if(ctxt.getMode()== RecommendationContext.MODE.EXCLUSION){
                exclusions = ctxt.getContextItems();
            }
            Integer dimId = getDimensionForAttrName(ctxt.getCurrentItem(), options);
            if (dimId != null)
            {
                JdoCountRecommenderUtils cUtils = new JdoCountRecommenderUtils(options.getName());
                CountRecommender r = cUtils.getCountRecommender(options.getName());
                if (r != null)
                {

                    long t1 = System.currentTimeMillis();
                    Map<Long, Double> recommendations = r.recommendGlobal(dimensionId, maxRecsCount, exclusions, options.getDecayRateSecs(), dimId);
                    long t2 = System.currentTimeMillis();
                    logger.debug("Recommendation via cluster counts for dimension "+dimId+" for item  "+ctxt.getCurrentItem()+" for user "+user+" took "+(t2-t1));
                    List<ItemRecommendationResultSet.ItemRecommendationResult> results = new ArrayList<>();
                    for (Map.Entry<Long, Double> entry : recommendations.entrySet()){
                        results.add(new ItemRecommendationResultSet.ItemRecommendationResult(entry.getKey(), entry.getValue().floatValue()));
                    }

                }
                else
                    logger.warn("Can't get count recommender for "+options.getName());
            }
            else
                logger.info("Can't get dim for item "+ctxt.getCurrentItem()+" so can't run cluster counts for dimension algorithm ");
        }
        else
            logger.info("Can't cluster count for category for user "+user+" client user id "+ctxt.getCurrentItem()+" as no current item passed in");

        return new ItemRecommendationResultSet(Collections.EMPTY_LIST);
    }

    @Override
    public String name() {
        return CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS_ITEM_CATEGORY.name();
    }
}
