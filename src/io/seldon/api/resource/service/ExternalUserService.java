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

package io.seldon.api.resource.service;

import com.restfb.exception.FacebookOAuthException;
import io.seldon.api.APIException;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ExtUserAttrType;
import io.seldon.api.resource.InteractionBean;
import io.seldon.api.resource.UserBean;
import io.seldon.facebook.FBConstants;
import io.seldon.facebook.client.AsyncFacebookClient;
import io.seldon.facebook.user.FacebookUser;
import io.seldon.general.ExternalUserAttribute;
import io.seldon.general.Interaction;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author philipince
 *         Date: 17/03/2014
 *         Time: 10:23
 */
@Service
public class ExternalUserService {

    private static final Logger LOG = Logger.getLogger(ExternalUserService.class);
    private static final String EXISTING_USER_QUERY = "select uid, is_app_user from user where uid = ";

    private final PersistenceProvider persister;
    private final UserServiceAdapter userService;
    private final AsyncFacebookClient fbClient;

    @Autowired
    public ExternalUserService(PersistenceProvider persister, UserServiceAdapter userService, AsyncFacebookClient fbClient) {
        this.persister = persister;
        this.userService = userService;
        this.fbClient = fbClient;
    }

    boolean deriveAndAddAttrsFromInteraction(ConsumerBean consumerBean, InteractionBean interaction) {
        // are we sharing with some unknown person
        if(interaction.getUser2() == null || !interaction.getUser2().matches("^[\\d]+$")){
            LOG.info("Not checking whether user2 is a user of the app in facebook as it is not a UID - user2="+interaction.getUser2());
            return true;

        }
        boolean isAppUser = queryFacebookForFriendStatus(consumerBean, interaction);
        persister.getExtUsrAttrPeer(consumerBean.getShort_name()).addUsrAttr(
                new ExternalUserAttribute(interaction.getUser2(), ExtUserAttrType.APP_USR.getTypeString(), String.valueOf(isAppUser)));
        return isAppUser;
    }

    private boolean queryFacebookForFriendStatus(ConsumerBean c, InteractionBean interactionBean) {
        String fbToken = userService.getToken(c, interactionBean.getUser1());
        if (fbToken == null) {
            LOG.warn("FB token was null, defaulting fbid " + interactionBean.getUser2() + " to isAppUser=true");
            return true;
        }
        try {
            List<FacebookUser> users = fbClient.executeFqlQuery(EXISTING_USER_QUERY + interactionBean.getUser2(), FacebookUser.class, fbToken);
            if (users.size() != 1) {
                LOG.error("");
                throw new APIException(APIException.GENERIC_ERROR);
            }
            return users.get(0).getIsAppUser();
        } catch (FacebookOAuthException e) {
            if (e.getMessage().contains("Session has expired")) {
                LOG.warn("Swallowed FB token expired exception awaiting /interaction changes ", e);
                return true;
            } else {
                throw e;
            }
        }
    }

    public Map<Interaction, Boolean> retrieveAttrByInteraction(String consumer, Collection<Interaction> interactions) {
        Map<Interaction, Boolean> toReturn = new HashMap<Interaction, Boolean>();
        Map<String, Interaction> uidToInteraction = new HashMap<String, Interaction>();

        for (Interaction interaction : interactions) {
            uidToInteraction.put(interaction.getUser2FbId(), interaction);
        }

        Map<String, Boolean> queryResult = persister.getExtUsrAttrPeer(consumer).retrieve(uidToInteraction.keySet(), ExtUserAttrType.APP_USR.getTypeString());
        for (Map.Entry<String, Boolean> singleResult : queryResult.entrySet()) {
            toReturn.put(uidToInteraction.get(singleResult.getKey()), singleResult.getValue());
        }
        return toReturn;
    }

    @Component
    public static class UserServiceAdapter {

        public String toExternalUserId(ConsumerBean consumer, long internalUserId) {
            return UserService.getClientUserId(consumer, internalUserId);
        }

        public String getToken(ConsumerBean c, String user) {
            UserBean userBean = UserService.getUserSafe(c, user, true);
            return userBean == null ? null : userBean.getAttributesName().get(FBConstants.FB_TOKEN);
        }
    }


}
