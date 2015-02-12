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

package io.seldon.facebook.importer;

import java.util.Date;
import java.util.Map;
import java.util.Observable;

import com.restfb.exception.*;
import com.restfb.types.User;
import io.seldon.api.logging.FacebookLogger;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.UserBean;
import io.seldon.facebook.Like;
import io.seldon.facebook.FacebookUserGraph;
import org.apache.log4j.Logger;

public class FacebookImporter extends Observable implements Runnable {

    private static final Logger logger = Logger.getLogger(FacebookImporter.class);
    private UserBean userBean;
    private ConsumerBean consumerBean;
    private String facebookId;
    private String fbToken;
    private FacebookImporterConfiguration configuration;
    private FacebookGraphCrawler importer;

    public FacebookGraphCrawler getImporter() {
        return importer;
    }

    public void setImporter(FacebookGraphCrawler importer) {
        this.importer = importer;
    }

    public String getFacebookId() {
        return facebookId;
    }

    public void setFacebookId(String facebookId) {
        this.facebookId = facebookId;
    }

    public String getFbToken() {
        return fbToken;
    }

    public void setFbToken(String fbToken) {
        this.fbToken = fbToken;
    }

    public FacebookImporterConfiguration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(FacebookImporterConfiguration configuration) {
        this.configuration = configuration;
    }

    public FacebookImporter(UserBean userBean, ConsumerBean consumerBean, String oauthToken, String facebookId) {
        this.fbToken = oauthToken;
        this.facebookId = facebookId;
        this.userBean = userBean;
        this.consumerBean = consumerBean;
        // Use the default configuration (which should just be likes)
        this.configuration = new FacebookImporterConfiguration();
    }

    public FacebookImporter(UserBean userBean, ConsumerBean consumerBean, String oauthToken, String facebookId,
                            FacebookImporterConfiguration configuration) {
        this.userBean = userBean;
        this.consumerBean = consumerBean;
        this.fbToken = oauthToken;
        this.facebookId = facebookId;
        this.configuration = configuration;
    }

    // TODO rework copied logic -- we shouldn't be returning null on failure.
    public FacebookUserGraph triggerImport() {
        boolean success = false;
        this.importer = new FacebookGraphCrawler(fbToken);

        logger.info("Firing up " + getClass().getName() + ": " + this);
        Date start = new Date();

        final String facebookId = String.valueOf(this.facebookId);
        FacebookUserGraph interests = null;
        try {
            interests = performImport();
            success = true;
        } catch (FacebookNetworkException e) {
            FacebookLogger.log(start, new Date(), facebookId, 0, FacebookLogger.Result.ERROR_NETWORK);
        } catch (FacebookOAuthException e) {
            FacebookLogger.log(start, new Date(), facebookId, 0, FacebookLogger.Result.ERROR_OAUTH);
        } catch (FacebookGraphException e) {
            FacebookLogger.log(start, new Date(), facebookId, 0, FacebookLogger.Result.ERROR_GRAPH);
        } catch (FacebookResponseStatusException e) {
            FacebookLogger.log(start, new Date(), facebookId, 0, FacebookLogger.Result.ERROR_FQL);
            if (e.getErrorCode() == 200) {
                System.out.println("Permission denied!");
            }
        } catch (FacebookException e) {
            FacebookLogger.log(start, new Date(), facebookId, 0, FacebookLogger.Result.ERROR);
        } catch (Exception e) {
            logger.error("[triggerImport] - Unexpected error", e);
        }

        String successful = (success ? "successful" : "unsuccessful");
        logger.info("[triggerImport] - user = " + this.facebookId + " - Completed=" + successful.toUpperCase());
        return interests;

    }

    private FacebookUserGraph performImport() {
        logger.info("FBIMP 0 - user = " + facebookId + " - Starting");
        Map<String, FacebookUserGraph> friends;
        Map<String, Like> likesMap;
        //Map<String, Like> groupsMap;
        FacebookUserGraph payload = new FacebookUserGraph();

        if (configuration.isLikes()) {
            likesMap = fetchLikes();
            payload.setLikes(likesMap);
        }

//        if (configuration.isGroups()) {
//            groupsMap = fetchGroups();
//            payload.setGroups(groupsMap);
//        }

        if (configuration.isFriends()) {
            friends = fetchFriends();
            payload.setFriends(friends);
        }

        if (configuration.isDemographics()) {
        	User user = fetchUser();
        	payload.setUser( user );
        }
        
        return payload;
    }

    private Map<String, Like> fetchLikes() {
        Map<String, Like> likesMap;
        likesMap = importer.fetchLikes(facebookId, configuration.getLikesLimit());
        logger.info("[Likes] - user = " + facebookId + " - Fetched likes: OK - Retrieved " + likesMap.size() + " Likes");
        return likesMap;
    }

    /*
    private Map<String, String> fetchGroups() {
        Map<String, String> groupsMap = null;
        try {
            groupsMap = importer.fetchMyGroups();
            logger.info("[Groups] - user = " + facebookId + " - Fetched groups: OK - Retrieved " + groupsMap.size() + " Groups");
        } catch (Exception e) {
            logger.error("[Groups] - Error in fetchGroups", e);
        }
        return groupsMap;
    }
    */
    
    private User fetchUser() {
    	User user = importer.fetchUser( facebookId );
    	return user;
    }

    private Map<String, FacebookUserGraph> fetchFriends() {
        Map<String, FacebookUserGraph> friends;
        final Boolean fDemographics = configuration.isfDemographics();
        final Boolean fLikes = configuration.isfLikes();
        final Boolean fGroups = configuration.isfGroups();
        final int likesLimit = configuration.getLikesLimit();
        final int distantLikesLimit = configuration.getDistantLikesLimit();
        final int friendsLimit = configuration.getFriendLimit();
        friends = importer.fetchFriends(facebookId, fDemographics, fLikes, fGroups, likesLimit, distantLikesLimit, friendsLimit);
        logger.info("[Friends] - user = " + facebookId + " - Fetched friends: OK - Retrieved " + friends.size() + " Friends");
        return friends;
    }

    @Override
    public String toString() {
        return "FacebookImporter{" +
                "importer=" + importer +
                ", facebookId='" + facebookId + '\'' +
                ", fbToken='" + fbToken + '\'' +
                ", configuration=" + configuration +
                '}';
    }

    @Override
    public void run() {
        // first, if we've been provided a user and consumer, ensure they exist

        final FacebookUserGraph userGraph = triggerImport();
        //final FacebookUserGraph userGraph = new FacebookUserGraph(); // 5/11/2013 - removing the FB fetch to get have just an empty FacebookUserGraph as it not not persisted and not needed.
        
        setChanged();
        notifyObservers(userGraph);
    }

    public UserBean getUserBean() {
        return userBean;
    }

    public void setUserBean(UserBean userBean) {
        this.userBean = userBean;
    }

    public ConsumerBean getConsumerBean() {
        return consumerBean;
    }

    public void setConsumerBean(ConsumerBean consumerBean) {
        this.consumerBean = consumerBean;
    }
}
