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
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.UserBean;
import io.seldon.api.resource.service.UserService;
import io.seldon.api.service.ApiLoggerServer;
import io.seldon.facebook.exception.FacebookDisabledException;
import io.seldon.facebook.service.FacebookService;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by: marc on 14/08/2012 at 10:56
 */
public class UserBusinessServiceImpl implements UserBusinessService {
    private static final Logger logger = Logger.getLogger(UserBusinessServiceImpl.class);

    @Autowired
    private FacebookService facebookService;




    @Override
    public ResourceBean updateUser(ConsumerBean consumerBean, UserBean userBean, String inviterCookie,boolean async, boolean fromAppRequest) {
        ResourceBean responseBean;
        
        try {
            boolean performImport = true;
            try {
                UserBean existingUser = UserService.getUser(consumerBean, userBean.getId(), true);

                if (existingUser != null && existingUser.getAttributesName() != null) {
                    final String facebookAttribute = existingUser.getAttributesName().get("facebook");
                    if (facebookAttribute != null) {
                        // Facebook import has already been triggered; don't reimport.
                        logger.info("[UserController#updateUser] Suppressing facebook import for user '" +
                                userBean.getId() + "'; consumer:" + consumerBean.getShort_name());
                        performImport = !(facebookAttribute.equals("1") || facebookAttribute.equals("true"));
                    }
                }
            } catch (APIException ignored) {
            }
            UserService.updateUser((ConsumerBean) consumerBean, userBean,async);
            //check if the user is connected to an external network (FB) and launch the import
            if (performImport) {
                try {
                    facebookService.performAsyncImport(consumerBean, userBean);
                    logger.info("Successfully triggered import for user: " + userBean.getId());
                } catch (FacebookDisabledException ignored) {
                    logger.warn("Import attempted for user: " + userBean.getId() + " but Facebook attribute is disabled.");
                }
            }
            responseBean = userBean;
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
