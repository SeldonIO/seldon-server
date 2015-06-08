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

import java.util.List;
import java.util.Set;

/**
 * Created by: marc on 14/08/2012 at 16:05
 */
public interface RecommendationBusinessService {

    ResourceBean recommendedItemsForUser(ConsumerBean consumerBean, String userId, int dimension, int limit);

    ResourceBean recommendedItemsForUser(ConsumerBean consumerBean, String userId, Long internalItemId, String uuid, int limit);

    ResourceBean recommendedItemsForUser(ConsumerBean consumerBean, String userId, Long internalItemId,
                                         Set<Integer> dimensions, String uuid, int limit, String attributes,
                                         List<String> algorithms, String referrer,String recTag, boolean includeCohort);

}
