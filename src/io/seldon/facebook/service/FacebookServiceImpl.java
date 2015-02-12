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

package io.seldon.facebook.service;

import java.util.Map;

import io.seldon.api.exception.UserMissingAttributesException;
import io.seldon.api.exception.UserNotFoundException;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.UserBean;
import io.seldon.facebook.FBConstants;
import io.seldon.facebook.exception.FacebookDisabledException;
import io.seldon.facebook.exception.FacebookIdException;
import io.seldon.facebook.exception.FacebookTokenException;
import io.seldon.facebook.importer.FacebookImporter;
import io.seldon.facebook.importer.FacebookImporterConfiguration;
import io.seldon.facebook.importer.FacebookImporterObserver;
import io.seldon.facebook.importer.FacebookOnlineImporterConfiguration;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

public class FacebookServiceImpl implements FacebookService {
    private static final Logger logger = Logger.getLogger(FacebookServiceImpl.class);

    @Autowired
    private FacebookImporterConfiguration facebookImporterConfiguration;

    @Autowired
    private FacebookImporterObserver importerObserver;

    public FacebookServiceImpl() {
    }

    private void performAsyncImport(ConsumerBean consumerBean, UserBean userBean, String facebookToken, String facebookId,
                                   FacebookImporterConfiguration configuration) {
        if (configuration.getFacebookEnabled()) {
        	if (FacebookOnlineImporterConfiguration.isOnlineImport(consumerBean.getShort_name()))
        	{
        		FacebookImporter facebookImporter = new FacebookImporter(userBean, consumerBean, facebookToken, facebookId, configuration);
        		importerObserver.addRunner(facebookImporter);
        	}
        	else
        		logger.info("Online FB import not active for client "+consumerBean.getShort_name()+" so not starting");
        } else {
            logger.info("Facebook import called but Facebook import not enabled.");
        }
    }

    @Override
    public void performAsyncImport(ConsumerBean consumerBean, UserBean userBean)
            throws UserNotFoundException, UserMissingAttributesException, FacebookDisabledException, FacebookIdException, FacebookTokenException {
        if (userBean == null) {
            throw new UserNotFoundException();
        }
        Map<String, String> userAttributes = userBean.getAttributesName();
        if (userAttributes == null) {
            return;
        }
        final String facebookFlag = userAttributes.get(FBConstants.FB);
        //check if the facebook parameter is present and it's true. otherwise it returns
        if ( facebookFlag == null ) {
            return;
        }
        boolean facebook = Boolean.parseBoolean(facebookFlag) || facebookFlag.equals("1");
        logger.info("Facebook attribute: " + facebook);
        if (!facebook) {
            throw new FacebookDisabledException();
        }
        String fbId = userAttributes.get(FBConstants.FB_ID);
        String fbToken = userAttributes.get(FBConstants.FB_TOKEN);
        if (fbId == null || fbId.length() == 0) {
            final String message = "User " + userBean.getId() + ": Facebook parameter active but fbId not provided.";
            final FacebookIdException facebookIdException = new FacebookIdException(userBean, fbId);
            logger.error(message, facebookIdException);
            throw facebookIdException;
        } else if ( fbToken == null || fbToken.length() == 0) {
            final String message = "User " + userBean.getId() + ": Facebook parameter active but fbToken not provided.";
            final FacebookTokenException facebookTokenException = new FacebookTokenException(userBean, fbToken);
            logger.error(message, facebookTokenException);
            throw facebookTokenException;
        }
        //Check if the user has already been imported.
        logger.info("Triggering async import for" + userBean.getId());
        performAsyncImport(consumerBean, userBean, fbToken, fbId, facebookImporterConfiguration);
    }

}