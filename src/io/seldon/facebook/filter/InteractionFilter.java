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

package io.seldon.facebook.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.RecommendedUserBean;
import io.seldon.api.resource.UserBean;
import io.seldon.api.resource.service.InteractionService;
import io.seldon.facebook.SocialRecommendationStrategy;
import io.seldon.facebook.user.FacebookUsersAlgorithm;
import io.seldon.general.Interaction;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.Multimap;

/**
 * @author philipince
 *         Date: 03/12/2013
 *         Time: 11:50
 */
@Component
public class InteractionFilter implements FacebookUsersAlgorithm {


    private static Logger logger = Logger.getLogger(InteractionFilter.class.getName());

    @Autowired
    private InteractionService interactionService;


    @Override
    public boolean shouldCacheResults() {
        return false;
    }


    @Override
    public List<RecommendedUserBean> recommendUsers(String userId, UserBean userbean, String serviceName, ConsumerBean client,
                                                    int resultLimit, Multimap<String, String> dict, SocialRecommendationStrategy.StrategyAim aim) {
        if(aim == SocialRecommendationStrategy.StrategyAim.SHARE_PAGES) return new ArrayList<>();
        Set<Interaction> interactions = interactionService.retrieveInteractions(client, userId, InteractionService.MGM_TYPE);
        return convertToRecUserBeans(client, interactions);
    }

    private List<RecommendedUserBean> convertToRecUserBeans(ConsumerBean client, Set<Interaction> interactions) {
        List<RecommendedUserBean> toReturn = new ArrayList<>();
        for(Interaction interaction : interactions){
            RecommendedUserBean userBean = new RecommendedUserBean(interaction.getUser2FbId(), null, 1.0D, null);
            toReturn.add(userBean);
        }
        logger.debug("Found "+toReturn.size() + " users to filter from results.");
        return toReturn;
    }
}
