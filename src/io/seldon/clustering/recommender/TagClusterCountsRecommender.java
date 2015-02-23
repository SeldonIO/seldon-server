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

import io.seldon.clustering.tag.TagClusterRecommender;
import io.seldon.clustering.tag.jdo.JdoItemTagCache;
import io.seldon.clustering.tag.jdo.JdoTagClusterCountStore;
import io.seldon.trust.impl.CFAlgorithm;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * @author firemanphil
 *         Date: 10/12/14
 *         Time: 13:57
 */
@Component
public class TagClusterCountsRecommender implements ItemRecommendationAlgorithm {
    @Override
    public ItemRecommendationResultSet recommend(CFAlgorithm options, String client, Long user, int dimensionId,
                                                 int maxRecsCount, RecommendationContext ctxt,
                                                 List<Long> recentItemInteractions) {
        if (ctxt.getCurrentItem() != null && options.isTagClusterCountsActive())
        {
            Set<Long> exclusions = Collections.emptySet();
            if(ctxt.getMode()== RecommendationContext.MODE.EXCLUSION){
                exclusions = ctxt.getContextItems();
            }
            TagClusterRecommender tagClusterRecommender = new TagClusterRecommender(options.getName(),
                    new JdoTagClusterCountStore(options.getName()),
                    new JdoItemTagCache(options.getName()),
                    options.getTagAttrId());
            Set<Long> userTagHistory = new HashSet<Long>();
            if (recentItemInteractions != null && options.getTagUserHistory() > 0)
            {
                if (recentItemInteractions.size() > options.getTagUserHistory())
                    userTagHistory.addAll(recentItemInteractions.subList(0, options.getTagUserHistory()));
                else
                    userTagHistory.addAll(recentItemInteractions);
            }
            Map<Long, Double> recommendations = tagClusterRecommender.recommend(user, ctxt.getCurrentItem(),
                    userTagHistory, dimensionId, maxRecsCount, exclusions, options.getDecayRateSecs());
            List<ItemRecommendationResultSet.ItemRecommendationResult> results = new ArrayList<>();
            for (Map.Entry<Long, Double> entry : recommendations.entrySet()){
                results.add(new ItemRecommendationResultSet.ItemRecommendationResult(entry.getKey(), entry.getValue().floatValue()));
            }
            return new ItemRecommendationResultSet(results);
        } else {
            return new ItemRecommendationResultSet(Collections.EMPTY_LIST);
        }
    }

    @Override
    public String name() {
        return CFAlgorithm.CF_RECOMMENDER.TAG_CLUSTER_COUNTS.name();
    }
}

