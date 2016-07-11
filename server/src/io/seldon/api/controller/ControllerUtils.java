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

package io.seldon.api.controller;

import javax.jdo.JDODataStoreException;

import org.apache.log4j.Logger;

import io.seldon.api.APIException;
import io.seldon.api.Constants;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.service.UserService;

public class ControllerUtils {

    private static Logger logger = Logger.getLogger(ControllerUtils.class.getName());

    public static long getInternalUserId(UserService userService, ConsumerBean consumerBean, String client_userId) {
        final String shortName = consumerBean.getShort_name();
        Long internalUserId;
        try {
            internalUserId = userService.getInternalUserId(consumerBean, client_userId);
        } catch (APIException e) {
            internalUserId = Constants.ANONYMOUS_USER;
        } catch (JDODataStoreException e) {
            logger.error("Got a datastore exception trying to get userid for " + client_userId + " client " + shortName, e);
            internalUserId = Constants.ANONYMOUS_USER;
        }
        return internalUserId;
    }

}
