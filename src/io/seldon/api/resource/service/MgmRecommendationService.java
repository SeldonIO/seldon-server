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

import java.util.*;

import com.google.common.collect.*;
import com.restfb.exception.FacebookResponseStatusException;
import io.seldon.api.Constants;
import io.seldon.api.resource.ListBean;
import io.seldon.api.resource.UserBean;
import io.seldon.facebook.FBConstants;
import io.seldon.facebook.SocialFriendsBlender;
import io.seldon.facebook.SocialFriendsScoreDecayFunction;
import io.seldon.facebook.SocialRecommendationStrategy;
import io.seldon.facebook.user.FacebookUsersAlgorithm;
import io.seldon.facebook.user.algorithm.SocialRecommendationStrategyStore;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.api.APIException;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.RecommendedUserBean;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Service
public class MgmRecommendationService {

    private static enum AlgorithmType {
        SIMILAR_USER, SIMILAR_TO_OTHER_SERVICE_USERS, LIKES_SIMILAR_SERVICES;
    }
    private static final String SERVICE_NAME = "MGM";
    private static Logger logger = Logger.getLogger(MgmRecommendationService.class.getName());

    @Resource(name = "baseSocialRecommendationStrategyStore")
    private SocialRecommendationStrategyStore strategyStore;

    @Autowired
    private ImpressionService impressionService;
    @Autowired
    private CacheExpireService cacheExpireService;

    public static final String DEFAULT_CLIENT = "default";


    public MgmRecommendationService() {}
    
    public ListBean getRecommendedUsers(ConsumerBean c, String userName, int limit, int usersShown, List<String> algorithmType, Multimap<String, String> algParams, String facebookToken, Boolean impressionEnabled) {
        
    	List<List<RecommendedUserBean>> algFriends = new ArrayList<>();
    	List<RecommendedUserBean> filteredFriends = new ArrayList<>();
        List<RecommendedUserBean> decayedFriends  = new ArrayList<>();
    	List<RecommendedUserBean> blendedFriends = new ArrayList<>();
        List<RecommendedUserBean> sortedFriends = new ArrayList<>();
    	ListBean resultFriends = new ListBean();
    	UserBean userBean = getUser(c,userName);
    	if (userBean == null)
    	{
    		logger.warn("Creating a dummy userBean for user "+userName);
    		userBean = new UserBean(userName,"dummy bean for "+userName);
    		userBean.setAttributesName(new HashMap<String, String>());
    	}
    	if (facebookToken != null) //overwrite token with token passed in if available
    		userBean.getAttributesName().put(FBConstants.FB_TOKEN, facebookToken);
    	
    	try
    	{

            logger.info("Using online recommendations for client " + c.getShort_name() + " and user " + userName);

            //load client configuration strategy from zookeeper
            logger.info("Loading zookeeper client configuration strategy for client " + c.getShort_name() + " and user " + userName);

            SocialRecommendationStrategy strategy = strategyStore.getStrategy(userBean.getId(), c.getShort_name());
            if(strategy==null) strategy = strategyStore.getStrategy(userBean.getId(), DEFAULT_CLIENT);

            //load algorithms
            List<FacebookUsersAlgorithm> algs = strategy.getOnlineAlgorithms();
	        
	        //load filters
	        List<FacebookUsersAlgorithm> exclusiveFilters = strategy.getExclusiveFilters();
            List<FacebookUsersAlgorithm> inclusiveFilters = strategy.getInclusiveFilters();
            if(exclusiveFilters==null) exclusiveFilters = new ArrayList<>();
            if(inclusiveFilters==null) inclusiveFilters = new ArrayList<>();

            //load blender
	        SocialFriendsBlender blender = strategy.getBlender();

            //load impression decay function
            List<SocialFriendsScoreDecayFunction> decayFunctions = strategy.getDecayFunctions();
            if(decayFunctions==null) decayFunctions = new ArrayList<>();



            // ---------------------- Algs -> Blender -> Filters -> Memcache -> Decay -> Sort ----------------------


            //get friend recommendations (after filters) from memcache if they exist
            //otherwise do fb calls and store filtered results in memcache

            String facebookUsersRecKey = MemCacheKeys.getFacebookUsersRecKey(SERVICE_NAME, c.getShort_name(), userBean.getId(), strategy.getUniqueCode(), algParams.hashCode());
            List<RecommendedUserBean> friendsMemCache = (List<RecommendedUserBean>) MemCachePeer.get(facebookUsersRecKey);


            if (friendsMemCache == null)
            {
                //friend recommendations are not in memcache, fb calls to retrieve them

                //pass friends through algorithms, filter them out and blend them
                logger.info("Getting friends from each algorithm for client " + c.getShort_name() + " and user " + userName);
                algFriends = getFriendsToRecommend(c, userBean, limit, algs, algParams, strategy.getAim());

                //blend recommended friends
                logger.info("Blending friends for client " + c.getShort_name() + " and user " + userName);
                blendedFriends = blender.blendFriends(algFriends);


                //pass friends through inclusive and exclusive filters
                if(inclusiveFilters.isEmpty())
                {
                    logger.info("No inclusive filters set for client " + c.getShort_name() + " and user " + userName);
                    filteredFriends = blendedFriends;
                }
                else
                {
                    logger.info("Getting friends for each inclusive filter for client " + c.getShort_name() + " and user " + userName);
                    List<List<RecommendedUserBean>> inclusiveFilterFriends = getFriendsToRecommend(c, userBean, limit, inclusiveFilters, algParams, strategy.getAim());
                    Set<RecommendedUserBean> streamlinedIncFilterFriends = streamline(inclusiveFilterFriends, false);

                    logger.info("Inclusive filtering friends for client " + c.getShort_name() + " and user " + userName);
                    filteredFriends = filterRecommendedFriends(blendedFriends,streamlinedIncFilterFriends, false);
                }

                if(!exclusiveFilters.isEmpty())
                {
                    logger.info("Getting friends for each exclusive filter for client " + c.getShort_name() + " and user " + userName);
                    List<List<RecommendedUserBean>> exclusiveFilterFriends = getFriendsToRecommend(c, userBean, limit, exclusiveFilters, algParams, strategy.getAim());
                    Set<RecommendedUserBean> streamlinedExcFilterFriends = streamline(exclusiveFilterFriends, true);

                    logger.info("Exclusive filtering friends for client " + c.getShort_name() + " and user " + userName);
                    filteredFriends = filterRecommendedFriends(filteredFriends,streamlinedExcFilterFriends, true);
                }


                //store after alg retrieval, blending, filtering friends in memcache
                if (!filteredFriends.isEmpty())
                {
                    logger.info("Number of friends after alg retrieval, blending, filtering from Facebook for client " + c.getShort_name() + " and user " + userName + " is " + filteredFriends.size());
                    cacheFriendsRecommendations(c, userBean, strategy, algParams, filteredFriends);
                }
            }
            else
            {
                //friend recommendations (after alg retrieval, blending, filtering) stored in memcache
                logger.info("Number of friends after alg retrieval, blending, filtering from memcache for client " + c.getShort_name() + " and user " + userName + " is " + friendsMemCache.size());
                filteredFriends = friendsMemCache;
            }




            //apply decay functions on friend scores
            decayedFriends = filteredFriends;
            if(decayFunctions.isEmpty())
            {
                logger.info("No decay functions set for client " + c.getShort_name() + " and user " + userName);
            }
            else
            {
                logger.info("Decaying friends scores for each decaying function for client " + c.getShort_name() + " and user " + userName);

                for (SocialFriendsScoreDecayFunction decayFunction : decayFunctions)
                {
                    logger.info("Decaying friends scores for decay function " + decayFunction.getClass().getName() + " for client " + c.getShort_name() + " and user " + userName);
                    decayedFriends = decayFunction.decayFriendsScore(c, userBean, decayedFriends);
                }
            }


            //sort decayed friends
            logger.info("Sorting friends by score for client " + c.getShort_name() + " and user " + userName);
            sortedFriends = sortFriendsByScore(decayedFriends);

	        //limit number of results to be returned
	        resultFriends.addAll(sortedFriends.subList(0, Math.min(limit, sortedFriends.size())));


            //apply impression count on the ones that are to be shown and update the impression memcache list
            if (impressionEnabled)
            {
                impressionService.increaseImpressions(c.getShort_name(), userBean.getId(), sortedFriends, usersShown);
            }
            else
            {
                impressionService.temporarilyStorePendingImpressions(c.getShort_name(), userBean.getId(), sortedFriends, usersShown);
            }

        }
    	catch (FacebookResponseStatusException frse) 
        {
            if (frse.getErrorCode() != null && frse.getErrorCode() == 1)
            {
                logger.info("Facebook Error Code #1 ... Failed facebook call for user " + userName + "Error: " + frse);
            }
            else
            {
                logger.error("NOT Facebook Error #1 ... Failed facebook call for user " + userName + "Error: " + frse);
                throw (new APIException(APIException.FACEBOOK_RESPONSE));
            }
        }
        
        return resultFriends;
    }










    private Set<RecommendedUserBean> streamline(List<List<RecommendedUserBean>> filterFriends, boolean isExclusive) {
        Set<RecommendedUserBean> streamlinedFilterFriends = new HashSet<>();
        if (isExclusive)
        {
            for(List<RecommendedUserBean> recs : filterFriends)
            {
                streamlinedFilterFriends.addAll(recs);
            }
        }
        else
        {
            if (!filterFriends.isEmpty())
            {
                streamlinedFilterFriends.addAll(filterFriends.get(0));
                for(List<RecommendedUserBean> recs : filterFriends)
                {
                    streamlinedFilterFriends.retainAll(recs);
                }
            }
        }
        return streamlinedFilterFriends;
    }


    private List<List<RecommendedUserBean>> getFriendsToRecommend(ConsumerBean c, UserBean userBean, int limit, List<? extends FacebookUsersAlgorithm> algs, Multimap<String, String> algParams, SocialRecommendationStrategy.StrategyAim aim)
    {
        //given a consumer, user and a list of algorithms with parameters, get friends recommended by each algorithm
        List<List<RecommendedUserBean>> recommendations = new ArrayList<>();
        for (FacebookUsersAlgorithm alg : algs)
        {
            List<RecommendedUserBean> results = alg.recommendUsers(userBean.getId(), userBean, SERVICE_NAME, c, 9999, algParams, aim);
            if (!results.isEmpty())
            {
                logger.info("Number of friends from Facebook for algorithm " + alg.getClass().getName() + " for user " + userBean.getId() + " is " + results.size());
                recommendations.add(results);
            }
        }

        return recommendations;
    }
    
    
    private List<RecommendedUserBean> filterRecommendedFriends(List<RecommendedUserBean> algsFriends, Set<RecommendedUserBean> friendsToFilter, boolean isExclusionFiltering)
    {
    	List<RecommendedUserBean> recommendedFriends = new ArrayList<>();

    	
    	List<RecommendedUserBean> filteredFriendsAlg = new ArrayList<>();
        for (RecommendedUserBean algsFriend : algsFriends)
        {
            boolean friendInFilter = friendsToFilter.contains(algsFriend);
            boolean keepFriend = (isExclusionFiltering && !friendInFilter) || (!isExclusionFiltering && friendInFilter);
            if (keepFriend)
            {
                recommendedFriends.add(algsFriend);
            }
        }

    	return recommendedFriends;
    }
    


    private List<RecommendedUserBean> sortFriendsByScore(List<RecommendedUserBean> friends)
    {
        Collections.sort(friends, new Comparator<RecommendedUserBean>()
        {
            public int compare(RecommendedUserBean r1, RecommendedUserBean r2)
            {
                return r2.getScore().compareTo(r1.getScore());
            }
        }
        );

        return friends;
    }


	private void cacheRecommendationAlgResults(ConsumerBean c, UserBean userBean, FacebookUsersAlgorithm alg, Multimap<String,String> algParams, List<RecommendedUserBean> results)
    {
	    String facebookUsersAlgRecKey = MemCacheKeys.getFacebookUsersAlgRecKey(SERVICE_NAME, c.getShort_name(), userBean.getId(), alg.getClass().getSimpleName(), algParams.hashCode());
	    if(Constants.CACHING) MemCachePeer.put(facebookUsersAlgRecKey, results, cacheExpireService.getCacheExpireSecs());
	}


    private void cacheFriendsRecommendations(ConsumerBean c, UserBean userBean, SocialRecommendationStrategy strategy, Multimap<String,String> algParams, List<RecommendedUserBean> results)
    {
        String facebookUsersRecKey = MemCacheKeys.getFacebookUsersRecKey(SERVICE_NAME, c.getShort_name(), userBean.getId(), strategy.getUniqueCode(), algParams.hashCode());
        if(Constants.CACHING) MemCachePeer.put(facebookUsersRecKey, results, cacheExpireService.getCacheExpireSecs());
    }



    // added this purely so I can test
  //  protected Long getInternalUserId(ConsumerBean conBean, String userId) {
  //      return UserService.getInternalUserId(conBean, userId);
  //  }

    protected UserBean getUser(ConsumerBean consumerBean, String userId){
    	try
    	{
    		return UserService.getUser(consumerBean, userId, true);
    	}
    	catch (APIException e)
    	{
    		if (e.getError_id() == APIException.USER_NOT_FOUND)
    		{
    			logger.warn("User not found with id "+userId+" for consumer "+consumerBean.getShort_name());
    			return null;
    		}
    		else
    			throw e;
    	}
    }


}
