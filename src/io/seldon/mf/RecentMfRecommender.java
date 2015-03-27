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
import io.seldon.clustering.recommender.MemcachedAssistedAlgorithm;
import io.seldon.clustering.recommender.RecommendationContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math.linear.ArrayRealVector;
import org.apache.commons.math.linear.RealVector;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.Ordering;

@Component
public class RecentMfRecommender implements ItemRecommendationAlgorithm {
 	private static Logger logger = Logger.getLogger(RecentMfRecommender.class.getName());
	private static final String name = RecentMfRecommender.class.getName();
	private static final String RECENT_ACTIONS_PROPERTY_NAME = "io.seldon.algorithm.general.numrecentactionstouse";
	private final MfFeaturesManager store;


	@Autowired
    public RecentMfRecommender(MfFeaturesManager store){
        this.store = store;
    }
    


    @Override
    public ItemRecommendationResultSet recommend(String client, Long user, int dimension,
            int maxRecsCount, RecommendationContext ctxt, List<Long> recentItemInteractions) {

		RecommendationContext.OptionsHolder opts = ctxt.getOptsHolder();
		int numRecentActionsToUse = opts.getIntegerOption(RECENT_ACTIONS_PROPERTY_NAME);
        MfFeaturesManager.ClientMfFeaturesStore clientStore = this.store.getClientStore(client);

        if(clientStore==null) {
            logger.debug("Couldn't find a matrix factorization store for this client");
            return new ItemRecommendationResultSet(Collections.<ItemRecommendationResult>emptyList(), name);
        }
        
        List<Long> itemsToScore;
		if(recentItemInteractions.size() > numRecentActionsToUse)
		{
			logger.debug("Limiting recent items for score to size "+numRecentActionsToUse+" from present "+recentItemInteractions.size());
			itemsToScore = recentItemInteractions.subList(0, numRecentActionsToUse);
		}
		else
			itemsToScore = new ArrayList<>(recentItemInteractions);

        double[] userVector;
        if (clientStore.productFeaturesInverse != null)
        {
        	//fold in user data from their recent history of item interactions
        	logger.debug("Creating user vector by folding in features");
        	userVector = foldInUser(itemsToScore, clientStore.productFeaturesInverse, clientStore.idMap);
        }
        else
        {
        	logger.debug("Creating user vector by averaging features");
        	userVector = createAvgProductVector(itemsToScore, clientStore.productFeatures);
        }
        
        Set<ItemRecommendationResult> recs = new HashSet<>();
        if(ctxt.getMode()== RecommendationContext.MODE.INCLUSION){
            // special case for INCLUSION as it's easier on the cpu.
            for (Long item : ctxt.getContextItems()){
            	if (!recentItemInteractions.contains(item))
            	{
            		float[] features = clientStore.productFeatures.get(item);
            		if(features!=null)
            			recs.add(new ItemRecommendationResult(item, dot(features,userVector)));
            	}
            }

        } else {
           for (Map.Entry<Long, float[]> productFeatures : clientStore.productFeatures.entrySet()) {
                Long item = productFeatures.getKey().longValue();
            	if (!recentItemInteractions.contains(item))
            	{
            		recs.add(new ItemRecommendationResult(item,dot(productFeatures.getValue(),userVector)));
            	}
           }
        }

        List<ItemRecommendationResult> recsList = Ordering.natural().greatestOf(recs, maxRecsCount);
        logger.debug("Created "+recsList.size() + " recs");
        return new ItemRecommendationResultSet(recsList, name);
    }

    public double[] createAvgProductVector(List<Long> recentitemInteractions,Map<Long,float[]> productFeatures)
    {
    	int numLatentFactors = productFeatures.values().iterator().next().length;
    	double[] userFeatures = new double[numLatentFactors];
    	for (Long item : recentitemInteractions) 
		{
    		float[] productFactors = productFeatures.get(item);
    		if (productFactors != null)
    		{
    			for (int feature = 0; feature < numLatentFactors; feature++) 
    			{
    				userFeatures[feature] += productFactors[feature];
    			}
    		}
		}
    	RealVector  userFeaturesAsVector = new ArrayRealVector(userFeatures);
    	RealVector normalised =  userFeaturesAsVector.mapDivide(userFeaturesAsVector.getL1Norm());

    	return normalised.getData();
    }
    
    /**
     * http://www.slideshare.net/fullscreen/srowen/matrix-factorization/16 
     * @param recentitemInteractions
     * @param productFeaturesInverse
     * @param idMap
     * @return
     */
    public double[] foldInUser(List<Long> recentitemInteractions,double[][] productFeaturesInverse,Map<Long,Integer> idMap) {

    	int numLatentFactors = productFeaturesInverse[0].length;
	    double[] userFeatures = new double[numLatentFactors];
	    for (Long item : recentitemInteractions) 
	    {
	    	Integer id = idMap.get(item);
	    	if (id != null)
	    	{
	    		for (int feature = 0; feature < numLatentFactors; feature++) 
	    		{
	    			userFeatures[feature] += productFeaturesInverse[id][feature];
	    		}
	    	}
	    }
	    return userFeatures;
	  }

    private static float dot(float[] vec1, double[] vec2){
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
