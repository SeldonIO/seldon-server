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
import io.seldon.facebook.FBConstants;
import io.seldon.facebook.user.FacebookUsersAlgorithm;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.google.common.collect.Multimap;
import com.restfb.DefaultFacebookClient;
import com.restfb.Parameter;
import io.seldon.api.resource.UserBean;
import io.seldon.facebook.SocialRecommendationStrategy;
import io.seldon.facebook.user.FacebookUser;

/**
 * Created with IntelliJ IDEA.
 * User: philipince
 * Date: 12/08/2013
 * Time: 12:12
 * To change this template use File | Settings | File Templates.
 */
@Component
public class OnlineCloseFriendsByMutualFriendsAlgorithm implements FacebookUsersAlgorithm {

    private static final Logger logger = Logger.getLogger(OnlineCloseFriendsByMutualFriendsAlgorithm.class);

    private static final String TOP_FRIENDS_BY_MUTUAL_FRIENDS_QUERY =
            "SELECT uid, mutual_friend_count "+
                    "FROM user " +
                    "WHERE uid IN (SELECT uid2 FROM friend WHERE uid1 = me())%s "+
                    "ORDER BY mutual_friend_count DESC limit 4000";
    private static final String MUTUAL_FRIENDS_REASON = "closeFriendsReason";


    @Override
    public List<RecommendedUserBean> recommendUsers(String userId, UserBean user, String service, ConsumerBean client, int resultLimit, Multimap<String, String> dict, SocialRecommendationStrategy.StrategyAim aim) {
        List<RecommendedUserBean> results = new ArrayList<>();
        long before = System.currentTimeMillis();

        if(user==null){
            logger.error("Couldn't find user with id "+ userId + " when recommending close friends.");
            return results;
        }
        String fbToken = user.getAttributesName().get(FBConstants.FB_TOKEN);
        if(fbToken==null){
            logger.error("User with id "+ userId + " does not have an fb token - return empty results set when recommending close friends.");
            return results;
        }
        DefaultFacebookClient facebookApiClient = new DefaultFacebookClient(fbToken);
        List<FacebookUser> userList = facebookApiClient.executeQuery(String.format(TOP_FRIENDS_BY_MUTUAL_FRIENDS_QUERY, FacebookAppUserFilterType.fromQueryParams(dict).toQuerySegment()), FacebookUser.class, new Parameter[0]);
        FacebookCallLogger fbCallLogger = new FacebookCallLogger();
        fbCallLogger.fbCallPerformed();
        fbCallLogger.log(this.getClass().getName(), client.getShort_name(), userId, fbToken);

        for(FacebookUser fbUser : userList){
            Long id = fbUser.getUid();
            Integer mutualFriends = fbUser.getMutualFriendsCount();
            results.add(new RecommendedUserBean(String.valueOf(id), null, new Double(mutualFriends), null, Arrays.asList(MUTUAL_FRIENDS_REASON)));
        }
        long timeTaken = System.currentTimeMillis() - before;
        logger.info("Retrieving top friends by mutual friends count took " + timeTaken + "ms for id "+userId);
        return results;
    }

    @Override
    public boolean shouldCacheResults() {
        return true;
    }




}
