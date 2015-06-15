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

package io.seldon.mf;

import io.seldon.clustering.recommender.ItemRecommendationAlgorithm;
import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.clustering.recommender.ItemRecommendationResultSet.ItemRecommendationResult;
import io.seldon.clustering.recommender.RecommendationContext;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.Ordering;

/**
 * @author firemanphil
 *         Date: 29/09/2014
 *         Time: 17:08
 */
@Component
public class MfRecommender implements ItemRecommendationAlgorithm {
    private static Logger logger = Logger.getLogger(MfRecommender.class.getName());
	private static final String name = MfRecommender.class.getSimpleName();
    private final MfFeaturesManager store;

    @Autowired
    public MfRecommender(MfFeaturesManager store){
        this.store = store;
    }

    @Override
    public ItemRecommendationResultSet recommend(String client, Long user, Set<Integer> dimensions, int maxRecsCount,
            RecommendationContext ctxt, List<Long> recentitemInteractions) {
        MfFeaturesManager.ClientMfFeaturesStore clientStore = this.store.getClientStore(client);

        if(clientStore==null || clientStore.userFeatures.get(user)==null) {
            logger.debug("Couldn't find a matrix factorization store for this client or this user");
            return new ItemRecommendationResultSet(Collections.<ItemRecommendationResult>emptyList(), name);
        }

        float[] userVector =  clientStore.userFeatures.get(user);
        Set<ItemRecommendationResult> recs = new HashSet<>();
        if(ctxt.getMode()== RecommendationContext.MODE.INCLUSION){
            // special case for INCLUSION as it's easier on the cpu.
            for (Long item : ctxt.getContextItems()){
            	if (!recentitemInteractions.contains(item))
            	{
            		float[] features = clientStore.productFeatures.get(item);
            		if(features!=null)
            			recs.add(new ItemRecommendationResult(item, dot(features,userVector)));
            	}
            }

        } else {
           for (Map.Entry<Long, float[]> productFeatures : clientStore.productFeatures.entrySet()) {
        	   Long item = productFeatures.getKey().longValue();
        	   if (!recentitemInteractions.contains(item))
          		{
        		   recs.add(new ItemRecommendationResult(item,dot(productFeatures.getValue(),userVector)));
          		}
           }
        }

        List<ItemRecommendationResult> recsList = Ordering.natural().greatestOf(recs, maxRecsCount);
        logger.debug("Created "+recsList.size() + " recs");
        return new ItemRecommendationResultSet(recsList, name);
    }


    private static float dot(float[] vec1, float[] vec2){
        float sum = 0;
        for (int i = 0; i < vec1.length; i++){
            sum += vec1[i] * vec2[i];
        }
        return sum;
    }

    @Override
    public String name() {
        return name;
    }
}
