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

package io.seldon.sv;

import io.seldon.clustering.recommender.ItemRecommendationAlgorithm;
import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.semvec.LongIdTransform;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author firemanphil
 *         Date: 22/02/15
 *         Time: 11:35
 */
@Component
public class SemanticVectorsRecommender implements ItemRecommendationAlgorithm {
    private static Logger logger = Logger.getLogger(SemanticVectorsRecommender.class.getName());
    private static final String IGNORE_PEFECT_MATCH_OPTION_NAME = "io.seldon.algorithm.semantic.ignoreperfectsvmatches";
    private static final String SV_PREFIX_OPTION_NAME = "io.seldon.algorithm.semantic.prefix";
    private static final String RECENT_ACTIONS_PROPERTY_NAME = "io.seldon.algorithm.general.numrecentactionstouse";
    private static final String name = SemanticVectorsRecommender.class.getSimpleName();
    SemanticVectorsManager svManager;


    @Autowired
    public SemanticVectorsRecommender(SemanticVectorsManager svManager)
    {
        super();
        this.svManager = svManager;
    }
    

   
    @Override
    public ItemRecommendationResultSet recommend(String client,Long user, Set<Integer> dimensions, int maxRecsCount, RecommendationContext ctxt,List<Long> recentItemInteractions) {

        RecommendationContext.OptionsHolder options = ctxt.getOptsHolder();
    	return recommendImpl(client, user, dimensions, ctxt, maxRecsCount, recentItemInteractions, options.getStringOption(SV_PREFIX_OPTION_NAME));
    }
    
   protected ItemRecommendationResultSet recommendImpl(String client,Long user, Set<Integer> dimensions, RecommendationContext ctxt, int maxRecsCount,List<Long> recentItemInteractions,String svPrefix) {
       RecommendationContext.OptionsHolder options = ctxt.getOptsHolder();
        if (recentItemInteractions.size() == 0)
        {
            logger.debug("Can't recommend as no recent item interactions");
            return new ItemRecommendationResultSet(Collections.<ItemRecommendationResultSet.ItemRecommendationResult>emptyList(), name);
        }
        Boolean isIgnorePerfectSvMatches = options.getBooleanOption(IGNORE_PEFECT_MATCH_OPTION_NAME);
        SemanticVectorsStore svPeer = svManager.getClientStore(client, svPrefix, ctxt);

        if (svPeer == null)
        {
            logger.debug("Failed to find sv peer for client "+client+" with type "+svPrefix);
            return new ItemRecommendationResultSet(Collections.<ItemRecommendationResultSet.ItemRecommendationResult>emptyList(), name);
        }
        
        RecommendationContext.OptionsHolder opts = ctxt.getOptsHolder();
		int numRecentActionsToUse = opts.getIntegerOption(RECENT_ACTIONS_PROPERTY_NAME);
		List<Long> itemsToScore;
		if(recentItemInteractions.size() > numRecentActionsToUse)
		{
			if (logger.isDebugEnabled())
				logger.debug("Limiting recent items for score to size "+numRecentActionsToUse+" from present "+recentItemInteractions.size());
			itemsToScore = recentItemInteractions.subList(0, numRecentActionsToUse);
		}
		else
			itemsToScore = new ArrayList<>(recentItemInteractions);
        
        Map<Long,Double> recommendations;


        if (ctxt.getMode() == RecommendationContext.MODE.INCLUSION)
        {
        	// compare itemsToScore against contextItems and choose best
        	logger.debug("inclusion mode ");
            recommendations = svPeer.recommendDocsUsingDocQuery(itemsToScore, ctxt.getContextItems() , new LongIdTransform(),maxRecsCount,isIgnorePerfectSvMatches);
        }
        else 
        {
        	//compare itemsToScore against all items and choose best ignoring exclusions
            Set<Long> itemExclusions = ctxt.getContextItems();
            if (logger.isDebugEnabled())
            	logger.debug("exclusion mode "+" num exclusions : "+itemExclusions.size());
            recommendations = svPeer.recommendDocsUsingDocQuery(itemsToScore, new LongIdTransform(), maxRecsCount, itemExclusions, null,isIgnorePerfectSvMatches);
        }
        List<ItemRecommendationResultSet.ItemRecommendationResult> results = new ArrayList<>();
        for(Map.Entry<Long, Double> e : recommendations.entrySet())
        {
            results.add(new ItemRecommendationResultSet.ItemRecommendationResult(e.getKey(), e.getValue().floatValue()));
        }
        return new ItemRecommendationResultSet(results, name);
    }

    @Override
    public String name() {
        return name;
    }
}
