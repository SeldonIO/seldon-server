/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.clustering.recommender;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

@Component
public class DynamicCategoryClusterCountsRecommender extends BaseItemCategoryRecommender implements ItemRecommendationAlgorithm {

	 private static Logger logger = Logger.getLogger(DynamicCategoryClusterCountsRecommender.class.getName());
	private static final String name = DynamicCategoryClusterCountsRecommender.class.getSimpleName();
	
    @Override
    public ItemRecommendationResultSet recommend(String client, Long user, Set<Integer> dimensions, int maxRecsCount,
                                                 RecommendationContext ctxt, List<Long> recentItemInteractions) {
    	if (ctxt.getCurrentItem() != null)
        {
    		Integer dimId = getDimensionForAttrName(ctxt.getCurrentItem(),client,ctxt);
    		if (dimId != null)
    		{
    			return this.recommend(name, "CLUSTER_COUNTS_CATEGORY_DYNAMIC",client,ctxt,user,dimensions,maxRecsCount,dimId);
    		}
    		else
    			logger.info("Can't get dim for item "+ctxt.getCurrentItem()+" so can't run cluster counts for dimension algorithm ");
        }
        else
            logger.info("Can't cluster count for category for user "+user+" client user id "+ctxt.getCurrentItem()+" as no current item passed in");

        return new ItemRecommendationResultSet(Collections.EMPTY_LIST, name);
    }

    @Override
    public String name() {
        return name;
    }

}
