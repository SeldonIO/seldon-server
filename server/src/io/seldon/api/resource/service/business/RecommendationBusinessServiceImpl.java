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

package io.seldon.api.resource.service.business;

import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.service.RecommendationService;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RecommendationBusinessServiceImpl implements RecommendationBusinessService {
    private static Logger logger = Logger.getLogger(RecommendationBusinessServiceImpl.class);

    @Autowired
    private RecommendationService recommendationService;

    @Override
    public ResourceBean recommendedItemsForUser(ConsumerBean consumerBean, String userId, int dimension, int limit) {
        return recommendationService.getRecommendedItems(consumerBean, userId, null, dimension, null, limit, null,null,null,null,false);
    }

    @Override
    public ResourceBean recommendedItemsForUser(ConsumerBean consumerBean, String userId, Long internalItemId, String uuid, int limit) {
        return recommendationService.getRecommendedItems(consumerBean, userId, internalItemId, 0, uuid, limit, null,null,null,null,false);
    }

    @Override
    public ResourceBean recommendedItemsForUser(ConsumerBean consumerBean, String userId, Long internalItemId,
                                                int dimensionId, String uuid, int limit, String attributes,List<String> algorithms,
                                                String referrer,String recTag, boolean includeCohort) {
        return recommendationService.getRecommendedItems(consumerBean, userId, internalItemId, dimensionId, uuid, limit, attributes,algorithms,referrer,recTag, includeCohort);
    }

  

   

}