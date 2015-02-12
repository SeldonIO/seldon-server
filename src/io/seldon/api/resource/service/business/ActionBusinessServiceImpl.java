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
import io.seldon.api.logging.MgmLogger;
import io.seldon.api.resource.ActionBean;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.service.ActionService;
import io.seldon.api.service.ApiLoggerServer;
import io.seldon.facebook.user.algorithm.experiment.MultiVariateTestStore;
import io.seldon.general.MgmAction;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

@Component
public class ActionBusinessServiceImpl implements ActionBusinessService {

    @Autowired
    MultiVariateTestStore testStore;

    @Autowired
    ActionService actionService;
    
    private static Logger logger = Logger.getLogger(ActionBusinessServiceImpl.class.getName());

    @Override
    public ResourceBean addAction(ConsumerBean consumerBean, ActionBean actionBean) {
        ResourceBean responseBean;
        try {
            actionService.addAction(consumerBean, actionBean);
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

    @Override
    public URI redirectToMgmInviteURL(ConsumerBean consumerBean, String url, String inviter, String userAgent) throws URISyntaxException {
        URI urlOfClick = new URI(url);

        urlOfClick = UriComponentsBuilder.fromUriString(urlOfClick.toString()).
                scheme(urlOfClick.getScheme() == null ? "http" : urlOfClick.getScheme()).build().toUri();
        MgmAction action = new MgmAction(inviter, null, new Date(), MgmAction.MgmActionType.FB_REC_CLICK, urlOfClick, null);

        // inform any running stats processes if its not a bot
        if (!BotUtils.isABot(userAgent)) {
            String mvTestVariationKey = null;
            if (testStore.testRunning(consumerBean.getShort_name())) {
                testStore.registerTestEvent(consumerBean.getShort_name(), action);
                mvTestVariationKey = testStore.retrieveVariationKey(consumerBean.getShort_name(), inviter);
            }


            // log action
            MgmLogger.log(consumerBean.getShort_name(), action, mvTestVariationKey);
        } else {
            logger.info("Not adding fb click thru to results as it has user-agent " + userAgent);
        }
        return urlOfClick;
    }

}