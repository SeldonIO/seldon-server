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
import java.util.Map;

import javax.annotation.Resource;

import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.RecommendedUserBean;
import io.seldon.facebook.user.FacebookUsersAlgorithm;
import io.seldon.similarity.dbpedia.WebSimilaritySimpleStore;
import io.seldon.similarity.dbpedia.jdo.SqlWebSimilaritySimplePeer;
import org.apache.log4j.Logger;

import com.google.common.collect.Multimap;
import io.seldon.api.resource.UserBean;
import io.seldon.api.resource.service.UserService;
import io.seldon.facebook.SocialRecommendationStrategy;
import io.seldon.similarity.dbpedia.WebSimilaritySimplePeer;
import io.seldon.trust.impl.SearchResult;

/**
 * Created with IntelliJ IDEA.
 * User: philipince
 * Date: 15/08/2013
 * Time: 10:29
 * To change this template use File | Settings | File Templates.
 */
public abstract class SimilarFriendsAlgorithm implements FacebookUsersAlgorithm {

    private static final String SIMILAR_FRIENDS_USER_KEY = "similarFriendsReason";

    private static Logger logger = Logger.getLogger(SimilarFriendsAlgorithm.class.getName());
    @Resource
    private Map<String, Integer> serviceNameToType;
    @Override
    public List<RecommendedUserBean> recommendUsers(String userId, UserBean userBean,String serviceName, ConsumerBean client, int resultLimit, Multimap<String, String> dict, SocialRecommendationStrategy.StrategyAim aim) {
        WebSimilaritySimpleStore wstore = new SqlWebSimilaritySimplePeer(client.getShort_name());
        WebSimilaritySimplePeer wpeer = new WebSimilaritySimplePeer(client.getShort_name(), wstore);
        Integer similarityType = serviceNameToType.get(serviceName);
        Long user = UserService.getInternalUserId(client,userId);
        if (user != null)
        {
        	List<SearchResult> similarUsers = wpeer.getSimilarUsers(user, resultLimit, getSimilarityMetric(), similarityType);
        	List<RecommendedUserBean> toReturn = new ArrayList<>(similarUsers.size());
        	for(SearchResult result : similarUsers){
        		RecommendedUserBean recommendedUserBean = new RecommendedUserBean(client, result);
        		recommendedUserBean.setReasons(Arrays.<String>asList(getMessageKey()));
        		toReturn.add(recommendedUserBean);
        	}
        	logger.info("Found " + toReturn.size() + " recs for similar users with service " + serviceName + " and metric type " + getSimilarityMetric());
        	return toReturn;
        }
        else
        {
        	logger.warn("No internal user id for user "+userId+" for client "+client);
        	return new ArrayList<>();
        }
    }

    protected abstract String getMessageKey();

    protected abstract int getSimilarityMetric();
}
