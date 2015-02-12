
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

import com.google.common.collect.Multimap;
import io.seldon.api.logging.FacebookCallLogger;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.RecommendedUserBean;
import io.seldon.api.resource.UserBean;
import io.seldon.facebook.FBConstants;
import io.seldon.facebook.SocialRecommendationStrategy;
import io.seldon.facebook.user.FacebookFriendsRanking;
import io.seldon.facebook.user.FacebookRandomFriendsGraph;
import io.seldon.facebook.user.FacebookRandomFriendsProcess;
import io.seldon.facebook.user.FacebookUsersAlgorithm;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Baseline Random friends algorithm that provides friends of the user on a random basis.
 * User: dylanlentini
 * Date: 1/11/2013
 * Time: 11:29
 */
@Component
public class OnlineFriendsByRandom implements FacebookUsersAlgorithm
{

    private static final Logger logger = Logger.getLogger(OnlineFriendsByRandom.class);
    
    private static final String RANDOM_FRIENDS_REASON = "randomFriendsReason";


    @Override
    public boolean shouldCacheResults() {
        return true;
    }

    @Override
    public List<RecommendedUserBean> recommendUsers(String userId, UserBean user, String service, ConsumerBean client, int resultLimit, Multimap<String, String> dict, SocialRecommendationStrategy.StrategyAim aim)
    {
        List<RecommendedUserBean> results = new ArrayList<RecommendedUserBean>();
        long before = System.currentTimeMillis();

        if(user==null)
        {
            logger.error("Couldn't find user with id "+ userId + " when recommending random friends.");
            return results;
        }
        String fbToken = user.getAttributesName().get(FBConstants.FB_TOKEN);
        if(fbToken==null)
        {
            logger.error("User with id "+ userId + " does not have an fb token - return empty results set when recommending random friends.");
            return results;
        }
        
        //get data from facebook
        FacebookCallLogger fbCallLogger = new FacebookCallLogger();
        FacebookRandomFriendsGraph myGraph = FacebookRandomFriendsGraph.build(fbToken, FacebookAppUserFilterType.fromQueryParams(dict),fbCallLogger);
        fbCallLogger.log(this.getClass().getName(), client.getShort_name(), userId, fbToken);
        
        //check for right permissions and retrieved data
        if (
        	myGraph.friendList != null &&
                    myGraph.permissions != null && myGraph.permissions.isEmpty() && myGraph.permissions.get(0).getUserFriendsPermission() == 1
                )
        {
        	//process fb data
        	List<FacebookFriendsRanking> friendsByRandom = new FacebookRandomFriendsProcess(myGraph).orderFriendsByRandom();
	        
        	//save results as a bean
	        for(FacebookFriendsRanking fbUserScore : friendsByRandom)
	        {
	        	results.add(new RecommendedUserBean(fbUserScore, null, null, Arrays.asList(RANDOM_FRIENDS_REASON)));
	        }
	        
	        long timeTaken = System.currentTimeMillis() - before;
	        logger.info("Retrieving friends by random count took " + timeTaken + "ms for id "+userId);
        }
        else
        {
        	if (myGraph.friendList == null)
        	{
	        	logger.error(userId + " friend list is empty.  Either FQL failed because of user_friends permission or user is anti-social, lonely, depressed and does not have any friends. For random friends.");
	        }
	        else
        	{
                if (myGraph.permissions == null || myGraph.permissions.isEmpty() || myGraph.permissions.get(0).getUserFriendsPermission() != 1) {
	            	logger.error(userId + " user does not have user_friends permission for random friends.");
	            }
        	}
        }
        
        
        return results;
    }






}
