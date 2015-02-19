
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

package io.seldon.facebook.user;

import com.restfb.DefaultFacebookClient;
import com.restfb.Facebook;
import com.restfb.FacebookClient;
import com.restfb.exception.FacebookResponseStatusException;
import io.seldon.api.logging.FacebookCallLogger;
import io.seldon.facebook.user.algorithm.FacebookAppUserFilterType;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Interactions between a user and his friends.
 * Class used to retrieve the data from the FB api.
 * @author dylanlentini
 *         Date: 28/10/2013
 *         Time: 12:45
 */
public class FacebookRandomFriendsGraph {


    private static final String GET_FRIENDS = "select uid from user where uid in (SELECT uid2 FROM friend WHERE uid1 = me() limit 4900)";
    private static final String GET_PERMISSIONS = "SELECT user_friends FROM permissions WHERE uid = me()";
    private static final Logger logger = Logger.getLogger(FacebookRandomFriendsGraph.class);

    public final List<FacebookUser> friendList;
    public final List<FacebookPermissions> permissions;

    private FacebookRandomFriendsGraph(List<FacebookUser> friends, List<FacebookPermissions> userPermissions)
    {
        this.friendList = friends;
        this.permissions = userPermissions;
    }

    public static FacebookRandomFriendsGraph build(String fbToken, FacebookAppUserFilterType appUserFilterType,
                                                   FacebookCallLogger fbCallLogger)
    {
        logger.info("Retrieving all facebook random friend with token " + fbToken);
        FacebookClient facebookApiClient = new DefaultFacebookClient(fbToken);
        MultiQueryResult result = null;

        List<FacebookUser> friends = null;
        List<FacebookPermissions> userPermissions = null;

        try 
        {
            fbCallLogger.fbCallPerformed();
            fbCallLogger.fbCallPerformed();
            Map<String, String> queries = new HashMap<>(2);
            queries.put("friends_query", GET_FRIENDS+appUserFilterType.toQuerySegment());
            queries.put("permissions_query", GET_PERMISSIONS);
            result = facebookApiClient.executeFqlMultiquery(queries, MultiQueryResult.class);
            
            friends = result.getFacebookFriends();
            userPermissions = result.getFacebookPermissions();
        } 
        catch (FacebookResponseStatusException frse) 
        {
            // this error code means that we requested TOO MUCH DATAS
            if (frse.getErrorCode() != null && frse.getErrorCode() == 1) 
            {
                logger.info("Failed to get facebook connections via multiquery call fbToken "+ fbToken);
            } 
            else 
            {
                logger.error("Problem when retrieving facebook connections for fbToken "+ fbToken, frse);
                throw frse;
            }
        }
        
        return new FacebookRandomFriendsGraph(friends, userPermissions);
    }
    
    
    public static class MultiQueryResult 
    {

    	@Facebook("friends_query")
        List<FacebookUser> friends_query;
        @Facebook("permissions_query")
        List<FacebookPermissions> permissions_query;
        
        public List<FacebookUser> getFacebookFriends()
        {
        	return friends_query;
        }
        
        public List<FacebookPermissions> getFacebookPermissions()
        {
        	return permissions_query;
        }
    }

}
