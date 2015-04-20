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
import io.seldon.api.logging.CtrFullLogger;
import io.seldon.api.resource.ActionBean;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.service.ActionService;
import io.seldon.api.resource.service.ItemService;
import io.seldon.api.resource.service.RecommendationService;
import io.seldon.api.service.ApiLoggerServer;
import io.seldon.recommendation.LastRecommendationBean;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ActionBusinessServiceImpl implements ActionBusinessService {

    @Autowired
    ActionService actionService;

    @Autowired
    ItemService itemService;

    @Autowired
    RecommendationService recService;
    
    private static Logger logger = Logger.getLogger(ActionBusinessServiceImpl.class.getName());

    @Override
    public ResourceBean addAction(ConsumerBean consumerBean, ActionBean actionBean,
                                  boolean isClickThrough, String recsCounter, String recTag) {
        ResourceBean responseBean;
        try {
            // add the action to storage
            actionService.addAction(consumerBean, actionBean);

            if(isClickThrough) {
                // register which recs were ignored for algorithm use
                LastRecommendationBean lastRecs = recService.retrieveLastRecs(consumerBean, actionBean, recsCounter);
                List<Long> ignoredItems = recService.findIgnoredItemsFromLastRecs(consumerBean, actionBean, lastRecs);
                itemService.updateIgnoredItems(consumerBean, actionBean, ignoredItems);
                // do logging
                actionService.logAction(consumerBean, actionBean, lastRecs, ignoredItems.size()+1, recTag, recsCounter);
            }

            responseBean = actionBean;
        } catch (APIException e) {
            ApiLoggerServer.log(this, e);
            responseBean = new ErrorBean(e);
        } catch (Exception e) {
            ApiLoggerServer.log(this, e);
            APIException apiEx = new APIException(APIException.INCORRECT_FIELD);
            responseBean = new ErrorBean(apiEx);
        }
        return responseBean;
    }

    

}