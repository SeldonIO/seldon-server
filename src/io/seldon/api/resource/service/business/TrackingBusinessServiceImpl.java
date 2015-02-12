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

import java.util.Date;

import io.seldon.api.APIException;
import io.seldon.api.logging.MgmLogger;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.SuccessBean;
import io.seldon.api.resource.service.ImpressionService;
import io.seldon.facebook.user.algorithm.experiment.MultiVariateTestStore;
import io.seldon.general.MgmAction;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TrackingBusinessServiceImpl implements TrackingBusinessService {

    private static Logger logger = Logger.getLogger(TrackingBusinessServiceImpl.class);

    @Autowired
    private MultiVariateTestStore mvTestStore;

    @Autowired
    private ImpressionService impressionService;

    @Override
    public ResourceBean trackEvent(String event, ConsumerBean consumerBean, String userId) {
        ResourceBean retVal = null;

        if ((event != null) && (event.length() > 0)) {

            if (event.equalsIgnoreCase("IMPRESSION")) {
                MgmAction action = new MgmAction(userId, null, new Date(), MgmAction.MgmActionType.IMPRESSION, null, null);
                String variationKey = null;
                if (mvTestStore.testRunning(consumerBean.getShort_name())) {
                    variationKey = mvTestStore.retrieveVariationKey(consumerBean.getShort_name(), userId);
                    mvTestStore.registerTestEvent(consumerBean.getShort_name(), action);
                }
                MgmLogger.log(consumerBean.getShort_name(), action, variationKey);
                impressionService.storeShownPendingImpressions(consumerBean.getShort_name(),userId);
            } else {
                MgmLogger.log(consumerBean.getShort_name(), event, userId, null);
            }
            retVal = new SuccessBean("OK");
        } else {
            retVal = new ErrorBean(new APIException(APIException.INCORRECT_FIELD));
        }

        return retVal;
    }

}
