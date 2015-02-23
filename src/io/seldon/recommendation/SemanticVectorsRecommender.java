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

import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.clustering.recommender.MemcachedAssistedAlgorithm;
import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.semvec.LongIdTransform;
import io.seldon.sv.SemanticVectorsManager;
import io.seldon.sv.SemanticVectorsStore;
import io.seldon.trust.impl.CFAlgorithm;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * @author firemanphil
 *         Date: 22/02/15
 *         Time: 11:35
 */
@Component
public class SemanticVectorsRecommender extends MemcachedAssistedAlgorithm {
    SemanticVectorsManager svManager;
    @Autowired
    public SemanticVectorsRecommender(SemanticVectorsManager svManager)
    {
        super();
        this.svManager = svManager;
    }

    @Override
    public ItemRecommendationResultSet recommendWithoutCache(CFAlgorithm options,String client,
                                                             Long user, int dimension, RecommendationContext ctxt, int maxRecsCount,List<Long> recentItemInteractions) {

        if (recentItemInteractions.size() == 0)
        {
            logger.debug("Can't recommend as no recent item interactions");
            return new ItemRecommendationResultSet(Collections.<ItemRecommendationResultSet.ItemRecommendationResult>emptyList());
        }

        SemanticVectorsStore svPeer = svManager.getStore(client, options.getSvPrefix());
        if (svPeer == null)
        {
            logger.debug("Failed to find sv peer for client "+client+" with type "+options.getSvPrefix());
            return new ItemRecommendationResultSet(Collections.<ItemRecommendationResultSet.ItemRecommendationResult>emptyList());
        }

        if(recentItemInteractions.size() > options.getTxHistorySizeForSV())
            recentItemInteractions = recentItemInteractions.subList(0, options.getTxHistorySizeForSV());

        Map<Long,Double> recommendations;


        if (options.getRecentArticlesForSV() > 0 || ctxt.getMode() == RecommendationContext.MODE.INCLUSION)
            recommendations = svPeer.recommendDocsUsingDocQuery(recentItemInteractions, ctxt.getContextItems() , new LongIdTransform(),maxRecsCount,options.isIgnorePerfectSVMatches());
        else {
            Set<Long> itemExclusions = ctxt.getMode() == RecommendationContext.MODE.INCLUSION ? Collections.<Long>emptySet() : ctxt.getContextItems();
            recommendations = svPeer.recommendDocsUsingDocQuery(recentItemInteractions, new LongIdTransform(), maxRecsCount, itemExclusions, null, options.isIgnorePerfectSVMatches());
        }
        List<ItemRecommendationResultSet.ItemRecommendationResult> results = new ArrayList<>();
        for(Map.Entry<Long, Double> e : recommendations.entrySet())
        {
            results.add(new ItemRecommendationResultSet.ItemRecommendationResult(e.getKey(), e.getValue().floatValue()));
        }
        return new ItemRecommendationResultSet(results);
    }

    @Override
    public String name() {
        return CFAlgorithm.CF_RECOMMENDER.SEMANTIC_VECTORS.name();
    }
}
