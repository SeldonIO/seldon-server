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
import io.seldon.facebook.user.*;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author philipince
 *         Date: 09/09/2013
 *         Time: 16:13
 */
@Component
public class OnlineCloseFriendsByCentrality implements FacebookUsersAlgorithm {


    private static final Logger logger = Logger.getLogger(OnlineCloseFriendsByCentrality.class);

    private static final String MUTUAL_FRIENDS_REASON = "closeFriendsReason";
    private final ThreadPoolExecutor executor;
    private final Boolean logStats;

    private FacebookFriendConnectionGraphBuilder builder;

    private OnlineCloseFriendsByMutualFriendsAlgorithm onlineCloseFriendsByMutualFriendsAlgorithm;

    @Autowired
    public OnlineCloseFriendsByCentrality(FacebookFriendConnectionGraphBuilder builder, @Value("${centrality.threads.core:1}") Integer coreThreads,
                                          @Value("${centrality.threads.max:1}") Integer maxThreads, @Value("${log.centrality.stats:false}") Boolean logStats) {
        this.builder = builder;
        this.logStats = logStats;
        this.executor = new ThreadPoolExecutor(coreThreads,maxThreads,10, TimeUnit.SECONDS,new ArrayBlockingQueue<Runnable>(10000,true));
    }

    @Override
    public List<RecommendedUserBean> recommendUsers(String userId, UserBean user, String serviceName, ConsumerBean client, int resultLimit, Multimap<String, String> dict, SocialRecommendationStrategy.StrategyAim aim) {
        logger.info("Retrieving close friends via online centrality algorithm for userid: "+userId + ", client: "+ client.getShort_name());
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
        FacebookAppUserFilterType appUserFilterType = FacebookAppUserFilterType.fromQueryParams(dict);


        FacebookCallLogger fbCallLogger = new FacebookCallLogger();
        FacebookFriendConnectionGraph myGraph = builder.build(fbToken, fbCallLogger);
        if (myGraph != null && myGraph.connections == null) {
            Collection<FacebookUser> filteredFriends = filterAppUsers(myGraph.friends, appUserFilterType);
            for (FacebookUser fbUser : filteredFriends) {
                Long id = fbUser.getUid();
                Integer mutualFriends = fbUser.getMutualFriendsCount();
                results.add(new RecommendedUserBean(String.valueOf(id), null, new Double(mutualFriends), null, Arrays.asList(MUTUAL_FRIENDS_REASON)));
            }
            return results;
        }
        fbCallLogger.log(this.getClass().getName(), client.getShort_name(), userId, fbToken);

        List<FacebookBetweenessCentrality.FacebookBetweennessCentralityResult> friendsByCentrality =
                new FacebookBetweenessCentrality(myGraph, logStats).orderFriendsByCentralityDesc(executor);
        Collection<FacebookBetweenessCentrality.FacebookBetweennessCentralityResult> filteredFriends = filterAppUsers(friendsByCentrality, appUserFilterType);
        for(FacebookBetweenessCentrality.FacebookBetweennessCentralityResult fbUser : filteredFriends){
            Long id = fbUser.getUid();
            Double score = fbUser.score;
            results.add(new RecommendedUserBean(String.valueOf(id), null, score, null, Arrays.asList(MUTUAL_FRIENDS_REASON)));
        }
        long timeTaken = System.currentTimeMillis() - before;
        logger.info("Retrieving top friends by centrality took " + timeTaken + "ms for id "+userId);
        return results;
    }

    private <T extends FacebookUser> Collection<T> filterAppUsers(List<T> friends, FacebookAppUserFilterType appUserFilterType) {
        List<T> toReturn = new ArrayList<>(friends.size());
            switch (appUserFilterType){
                case NONE:
                    return friends;
                case REMOVE_APP_USERS:
                    for(T friend : friends){
                        if(friend.getIsAppUser()==null || !friend.getIsAppUser()){

                            toReturn.add(friend);
                        }
                    }
                    break;
                case REMOVE_NON_APP_USERS:
                    for(T friend : friends){
                        if(friend.getIsAppUser()==null || friend.getIsAppUser()){
                            toReturn.add(friend);
                        }
                    }
                    break;

        }
        return toReturn;
    }

    @Override
    public boolean shouldCacheResults() {
        return true;
    }

    @PreDestroy
    public void shutdown(){
        executor.shutdownNow();
    }
}