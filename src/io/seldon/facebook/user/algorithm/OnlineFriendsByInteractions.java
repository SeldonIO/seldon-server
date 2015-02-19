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

package io.seldon.facebook.user.algorithm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.seldon.api.logging.FacebookCallLogger;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.RecommendedUserBean;
import io.seldon.api.resource.UserBean;
import io.seldon.facebook.FBConstants;
import io.seldon.facebook.SocialRecommendationStrategy;
import io.seldon.facebook.user.FacebookFriendsRanking;
import io.seldon.facebook.user.FacebookInteractionsGraphStatusPhotoLike;
import io.seldon.facebook.user.FacebookInteractionsProcessStatusPhotoLike;
import io.seldon.facebook.user.FacebookUsersAlgorithm;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.google.common.collect.Multimap;

/**
 * Interactions algorithm that provides the users that liked statuses and photos of the user.
 * User: dylanlentini
 * Date: 25/10/2013
 * Time: 17:15
 */
@Component
public class OnlineFriendsByInteractions implements FacebookUsersAlgorithm
{

    private static final Logger logger = Logger.getLogger(OnlineFriendsByInteractions.class);
    
    private static final String INTERACTIONS_FRIENDS_REASON = "interactionsFriendsReason";


    @Override
    public List<RecommendedUserBean> recommendUsers(String userId, UserBean user, String service, ConsumerBean client, int resultLimit, Multimap<String, String> dict, SocialRecommendationStrategy.StrategyAim aim)
    {
        List<RecommendedUserBean> results = new ArrayList<>();
        long before = System.currentTimeMillis();

        if(user==null)
        {
            logger.error("Couldn't find user with id "+ userId + " when recommending friends by interactions.");
            return results;
        }
        String fbToken = user.getAttributesName().get(FBConstants.FB_TOKEN);
        if(fbToken==null)
        {
            logger.error("User with id "+ userId + " does not have an fb token - return empty results set when recommending friends by interactions.");
            return results;
        }
        
        //get data from facebook
        FacebookCallLogger fbCallLogger = new FacebookCallLogger();
        FacebookInteractionsGraphStatusPhotoLike myGraph = FacebookInteractionsGraphStatusPhotoLike.build(fbToken,fbCallLogger);
        fbCallLogger.log(this.getClass().getName(), client.getShort_name(), userId, fbToken);
        
        //check for right permissions and retrieved data
        if (
        	myGraph.userStatusLikeList != null &&
        	myGraph.userPhotoLikeList != null  && 
        	myGraph.permissions.get(0).getUserStatusPermission() == 1 && 
        	myGraph.permissions.get(0).getUserPhotosPermission() == 1
        )
        {
        	//process fb data
        	List<FacebookFriendsRanking> friendsByInteractions = new FacebookInteractionsProcessStatusPhotoLike(myGraph).orderFriendsByInteractions();
	        
        	//save results as a bean
	        for(FacebookFriendsRanking fbUserScore : friendsByInteractions)
	        {
	        	results.add(new RecommendedUserBean(fbUserScore, null, null, Arrays.asList(INTERACTIONS_FRIENDS_REASON)));
	        }
	        
	        long timeTaken = System.currentTimeMillis() - before;
	        logger.info("Retrieving top friends by interactions count took " + timeTaken + "ms for id "+userId);
        }
        else
        {
        	if ((myGraph.userStatusLikeList == null) ||  (myGraph.userPhotoLikeList == null))
        	{
	        	if (myGraph.userStatusLikeList == null)
	        	{
	        		logger.error(userId + " user status list is empty.  Either FQL failed because of user_status permission or user does not post statuses or his statuses are not liked by anyone.");
	        	}
	        	if (myGraph.userPhotoLikeList == null)
	        	{
	        		logger.error(userId + " user photo list is empty.  Either FQL failed because of user_photos permission or user does not post photos or his photos are not liked by anyone.");
	        	}
        	}
        	else
        	{
	            if (myGraph.permissions.get(0).getUserStatusPermission() != 1)
	            {
	            	logger.error(userId + " user does not have user_status permission.");
	            }
	            if (myGraph.permissions.get(0).getUserPhotosPermission() != 1)
	            {
	            	logger.error(userId + " user does not have user_photo permission.");
	            }
        	}
        }
        
        
        return results;
    }



    @Override
    public boolean shouldCacheResults() {
        return true;
    }



}
