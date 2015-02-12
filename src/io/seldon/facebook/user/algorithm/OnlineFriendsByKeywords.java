
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
import io.seldon.facebook.client.AsyncFacebookClient;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.RecommendedUserBean;
import io.seldon.api.resource.UserBean;
import io.seldon.facebook.FBConstants;
import io.seldon.facebook.SocialRecommendationStrategy;
import io.seldon.facebook.user.FacebookFriendsKeywordsGraph;
import io.seldon.facebook.user.FacebookFriendsKeywordsProcess;
import io.seldon.facebook.user.FacebookFriendsRanking;
import io.seldon.facebook.user.FacebookUsersAlgorithm;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.Multimap;

/**
 * Friends by keywords algorithm that provides friends of the user who like pages that include those particular keywords (not exclusive).
 * User: dylanlentini
 * Date: 22/11/2013
 * Time: 11:29
 */
@Component
public class OnlineFriendsByKeywords implements FacebookUsersAlgorithm
{

    private static final Logger logger = Logger.getLogger(OnlineFriendsByKeywords.class);
    
    private static final String KEYWORDS_FRIENDS_REASON = "keywordsFriendsReason";

    @Autowired
    private AsyncFacebookClient asyncFacebookClient;

    @Override
    public boolean shouldCacheResults() {
        return true;
    }
    
    
    @Override
    public List<RecommendedUserBean> recommendUsers(String userId, UserBean user, String service, ConsumerBean client, int resultLimit, Multimap<String,String> algParams, SocialRecommendationStrategy.StrategyAim aim)
    {
        List<RecommendedUserBean> results = new ArrayList<RecommendedUserBean>();
        long before = System.currentTimeMillis();

        if(user==null)
        {
            logger.error("Couldn't find user with id "+ userId + " when recommending friends by keywords.");
            return results;
        }
        String fbToken = user.getAttributesName().get(FBConstants.FB_TOKEN);
        if(fbToken==null)
        {
            logger.error("User with id "+ userId + " does not have an fb token - return empty results set when recommending friends by keywords.");
            return results;
        }

        FacebookCallLogger fbCallLogger = new FacebookCallLogger();

        //get data from facebook
        FacebookFriendsKeywordsGraph myGraph = FacebookFriendsKeywordsGraph.build(fbToken, algParams, fbCallLogger, asyncFacebookClient);
        fbCallLogger.log(this.getClass().getName(), client.getShort_name(), userId, fbToken);
        
        //check for retrieved data
        if (myGraph.friendsPagesFans != null)
        {
        	//process fb data
        	List<FacebookFriendsRanking> friendsByKeywords = new FacebookFriendsKeywordsProcess(myGraph).orderFriendsByKeywords();
	        
        	//save results as a bean
	        for(FacebookFriendsRanking fbUserScore : friendsByKeywords)
	        {
	            results.add(new RecommendedUserBean(fbUserScore, null, null, Arrays.asList(KEYWORDS_FRIENDS_REASON)));
	        }
	        
	        long timeTaken = System.currentTimeMillis() - before;
	        logger.info("Retrieving top friends by keywords took " + timeTaken + "ms for id "+userId);
        }
        else
        {
           	logger.error(userId + " friend list is empty.  Either FQL failed because of user_likes, friends_likes permissions or user's friends are sad and depressed who do not like anything.");
	    }
	            
        return results;
    }






}
