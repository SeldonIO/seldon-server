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

import java.util.List;

import com.google.common.collect.Multimap;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.RecommendedUserBean;
import io.seldon.api.resource.UserBean;
import io.seldon.facebook.SocialRecommendationStrategy;


/**
 * Created with IntelliJ IDEA.
 * User: philipince
 * Date: 12/08/2013
 * Time: 12:01
 * To change this template use File | Settings | File Templates.
 */
public interface FacebookUsersAlgorithm {

    public List<RecommendedUserBean> recommendUsers(String userId, UserBean user, String serviceName, ConsumerBean client, int resultLimit, Multimap<String, String> dict, SocialRecommendationStrategy.StrategyAim aim);
    boolean shouldCacheResults();
}
