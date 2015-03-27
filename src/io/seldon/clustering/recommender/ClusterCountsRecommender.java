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

import java.util.List;

import org.springframework.stereotype.Component;

/**
 * @author firemanphil
 *         Date: 10/12/14
 *         Time: 11:18
 */
@Component
public class ClusterCountsRecommender extends BaseClusterCountsRecommender implements ItemRecommendationAlgorithm {

	private static final String name = ClusterCountsRecommender.class.getSimpleName();
	
    @Override
    public ItemRecommendationResultSet recommend(String client, Long user, int dimensionId, int maxRecsCount, RecommendationContext ctxt, List<Long> recentItemInteractions) {
        return this.recommend(name,"CLUSTER_COUNTS",client,ctxt,user,dimensionId,maxRecsCount);
    }

    @Override
    public String name() {
        return name;
    }
}
