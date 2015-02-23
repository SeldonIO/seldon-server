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

import io.seldon.api.APIException;
import io.seldon.api.Constants;
import io.seldon.api.Util;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.service.RecommendationService;
import io.seldon.api.service.ApiLoggerServer;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RecommendationBusinessServiceImpl implements RecommendationBusinessService {
    private static Logger logger = Logger.getLogger(RecommendationBusinessServiceImpl.class);

    @Autowired
    private RecommendationService recommendationService;

    @Override
    public ResourceBean recommendedItemsForUser(ConsumerBean consumerBean, String userId, int limit) {
        return recommendationService.getRecommendedItems(consumerBean, userId, null, 0, null, limit, null,null,null,null);
    }

    @Override
    public ResourceBean recommendedItemsForUser(ConsumerBean consumerBean, String userId, Long internalItemId, String uuid, int limit) {
        return recommendationService.getRecommendedItems(consumerBean, userId, internalItemId, 0, uuid, limit, null,null,null,null);
    }

    @Override
    public ResourceBean recommendedItemsForUser(ConsumerBean consumerBean, String userId, Long internalItemId, int dimensionId, String uuid, int limit, String attributes,List<String> algorithms,String referrer,String recTag) {
        return recommendationService.getRecommendedItems(consumerBean, userId, internalItemId, dimensionId, uuid, limit, attributes,algorithms,referrer,recTag);
    }

    @Override
    public ResourceBean recommendationsForUser(ConsumerBean consumerBean, HttpServletRequest request, String userId) {
        ResourceBean res;
        try {
            List<String> keywords = Util.getKeywords(request);
            Integer itemType = Util.getType(request);
            Integer dimension = Util.getDimension(request);
            List<String> algorithms = Util.getAlgorithms(request);
            /*if(dimension == null) {
                if(itemType == null) {
                    dimension = Constants.DEFAULT_DIMENSION;
                }
                else {
                    dimension = ItemService.getDimensionbyItemType((ConsumerBean)con, itemType);
                }
            }*/
            if (dimension == null) {
                dimension = Constants.DEFAULT_DIMENSION;
            }
            //with specific keywords
            if (keywords != null) {
                res = recommendationService.getRecommendations(consumerBean, userId, keywords, Util.getLimit(request), Util.getFull(request), dimension, algorithms);
            } else {
                int limit = Util.getLimit(request);
                boolean full = Util.getFull(request);
                res = recommendationService.getRecommendations(consumerBean, userId, itemType, dimension, limit, full, algorithms);
            }
        } catch (APIException e) {
            ApiLoggerServer.log(this, e);
            res = new ErrorBean(e);
        } catch (NullPointerException e) {
            logger.error("NullPointer", e);
            ApiLoggerServer.log(this, e);
            APIException apiEx = new APIException(APIException.RESOURCE_NOT_FOUND);
            res = new ErrorBean(apiEx);
        } catch (Exception e) {
            ApiLoggerServer.log(this, e);
            APIException apiEx = new APIException(APIException.GENERIC_ERROR);
            res = new ErrorBean(apiEx);
        }
        return res;
    }

   

}