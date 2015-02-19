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

import io.seldon.api.APIException;

import io.seldon.api.logging.FacebookCallLogger;
import io.seldon.facebook.client.AsyncFacebookClient;
import io.seldon.facebook.user.algorithm.FacebookAppUserFilterType;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import com.google.common.collect.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Get pages that contain a specific keyword, sorted by fan count.  Get friends who like those pages.
 * Class used to retrieve the data from the FB api.
 * @author dylanlentini
 *         Date: 28/10/2013
 *         Time: 12:45
 */
public class FacebookFriendsKeywordsGraph {


    private static final String GET_PAGES = "SELECT page_id FROM page WHERE contains('%s') ORDER BY fan_count DESC";
    private static final String GET_FRIENDS = "SELECT uid FROM user WHERE uid IN (SELECT uid2 FROM friend WHERE uid1 = me()) %s";
    private static final String GET_FRIENDS_PAGES = "SELECT uid,type,page_id FROM page_fan WHERE page_id IN (%s) AND uid IN (SELECT uid2 FROM friend WHERE uid1 = me())";

    private static final Logger logger = Logger.getLogger(FacebookFriendsKeywordsGraph.class);


    public final List<FacebookFriendPageFan> friendsPagesFans;

    private FacebookFriendsKeywordsGraph(List<FacebookFriendPageFan> friendsPagesFansList)
    {
        this.friendsPagesFans = friendsPagesFansList;
    }
    
    

    public static FacebookFriendsKeywordsGraph build(String fbToken, Multimap<String,String> algParams, FacebookCallLogger fbCallLogger, AsyncFacebookClient asyncFacebookClient)
    {
        logger.info("Retrieving facebook friends by keywords with token " + fbToken);
        
        List<FacebookFriendPageFan> friendsPagesFansList = null;
        List<FacebookPermissions> userPermissions = null;

        Collection<String> keywords = algParams.get("keywords");

        if (keywords.isEmpty())
        {
        	//no keywords, return empty list
        	logger.info("Retrieving facebook friends by keywords failed because of empty list of keywords");
        	friendsPagesFansList = new ArrayList<>();
        }
        else
        {
        	logger.info("Retrieving facebook friends by keywords " + keywords.toString());
            FacebookPagesFriendsResult result = getPagesFriendsFromFacebook(keywords, FacebookAppUserFilterType.fromQueryParams(algParams), fbToken, fbCallLogger, asyncFacebookClient);
            List<FacebookPage> pagesPerKeyword = result.getPages();
            List<FacebookUser> friends = result.getFriends();
        	List<String> pagesList = formatPagesInStringList(pagesPerKeyword);
            Set<String> friendsList = formatFriendsInStringSet(friends);
        	friendsPagesFansList = getFriendsPagesFansFromFacebook(pagesList,friendsList,fbToken,fbCallLogger, asyncFacebookClient);
        }
        
        return new FacebookFriendsKeywordsGraph(friendsPagesFansList);
    }


    
    private static FacebookPagesFriendsResult getPagesFriendsFromFacebook(Collection<String> keywords, FacebookAppUserFilterType appUserFilterType, String fbToken, FacebookCallLogger fbCallLogger, AsyncFacebookClient asyncFacebookClient)
    {
        List<FacebookPage> pages = new ArrayList<>();
        List<Future<List<FacebookPage>>> futurePages = new ArrayList<>();

        List<FacebookUser> friends = new ArrayList<>();
        List<Future<List<FacebookUser>>> futureFriends = new ArrayList<>();

        for (int i=0; i<5; i++)
        {
            String fql = String.format(GET_FRIENDS,appUserFilterType.toQuerySegment()) + " LIMIT 1000 " + (i==0?"":("OFFSET "+i*1000));
            futureFriends.add(asyncFacebookClient.executeFqlQueryAsync(fql, FacebookUser.class, fbToken));
            fbCallLogger.fbCallPerformed();
        }

        for (String keyword : keywords)
        {
            String fql = String.format(GET_PAGES, keyword);
            futurePages.add(asyncFacebookClient.executeFqlQueryAsync(fql, FacebookPage.class, fbToken));
            fbCallLogger.fbCallPerformed();
        }


        for(Future<List<FacebookUser>> futureFriend : futureFriends){
            try {
                friends.addAll(futureFriend.get());
            } catch (InterruptedException e) {
                logger.error("Problem querying facebook friends for access token '"+fbToken+"'", e);
                throw new APIException(APIException.GENERIC_ERROR);
            } catch (ExecutionException e) {
                logger.error("Problem querying facebook friends for access token '" + fbToken + "'", e);
                throw new APIException(APIException.GENERIC_ERROR);
            }
        }

        for(Future<List<FacebookPage>> futurePage : futurePages){
            try {
                pages.addAll(futurePage.get());
            } catch (InterruptedException e) {
                logger.error("Problem querying facebook friends for access token '"+fbToken+"'", e);
                throw new APIException(APIException.GENERIC_ERROR);
            } catch (ExecutionException e) {
                logger.error("Problem querying facebook friends for access token '" + fbToken + "'", e);
                throw new APIException(APIException.GENERIC_ERROR);
            }
        }

        return new FacebookPagesFriendsResult(pages,friends);
    }


    private static List<String> formatPagesInStringList(List<FacebookPage> pagesPerKeyword)
    {
    	List<String> pagesList = new ArrayList<>();
    	for (FacebookPage page : pagesPerKeyword) pagesList.add(page.getPageId().toString());    		
    	return pagesList;
    }

    private static Set<String> formatFriendsInStringSet(List<FacebookUser> friends)
    {
        Set<String> friendsList = new HashSet<>();
        for (FacebookUser friend : friends) friendsList.add(friend.getUid().toString());
        return friendsList;
    }
    
    private static List<FacebookFriendPageFan> getFriendsPagesFansFromFacebook(List<String> pagesList, Set<String> friendsList, String fbToken, FacebookCallLogger fbCallLogger, AsyncFacebookClient asyncFacebookClient)
    {
        List<FacebookFriendPageFan> friendsPagesFans = new ArrayList<>();
        List<Future<List<FacebookFriendPageFan>>> futures = new ArrayList<>();

        String fql = String.format(GET_FRIENDS_PAGES, StringUtils.join(pagesList, ','));
        futures.add(asyncFacebookClient.executeFqlQueryAsync(fql, FacebookFriendPageFan.class, fbToken));
        fbCallLogger.fbCallPerformed();

        for(Future<List<FacebookFriendPageFan>> future : futures){
            try {
                friendsPagesFans.addAll(future.get());
            } catch (InterruptedException e) {
                logger.error("Problem querying facebook friends for access token '"+fbToken+"'", e);
                throw new APIException(APIException.GENERIC_ERROR);
            } catch (ExecutionException e) {
                logger.error("Problem querying facebook friends for access token '" + fbToken + "'", e);
                throw new APIException(APIException.GENERIC_ERROR);
            }
        }


        List<FacebookFriendPageFan> filteredFriendsPages = new ArrayList<>();
        for (FacebookFriendPageFan friendPageFan : friendsPagesFans)
        {
            if (friendsList.contains(friendPageFan.getUid().toString()))
            {
                filteredFriendsPages.add(friendPageFan);
            }
        }

        return  filteredFriendsPages;
    }

}

