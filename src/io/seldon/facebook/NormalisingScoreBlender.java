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

package io.seldon.facebook;

import io.seldon.api.resource.RecommendedUserBean;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * @author philipince
 *         Date: 12/02/2014
 *         Time: 10:05
 */
@Component
public class NormalisingScoreBlender implements SocialFriendsBlender {

    private static final Logger logger = Logger.getLogger(NormalisingScoreBlender.class);

    @Override
    public List<RecommendedUserBean> blendFriends(List<List<RecommendedUserBean>> rankedFriendsLists) {
        int noOfAlgs = rankedFriendsLists.size();
        logger.info("Considering "+noOfAlgs+" algorithm's results. ");
        Map<String, RecommendedUserBean> toReturn = new HashMap<>();
        for(List<RecommendedUserBean> recommendedUserBeans : rankedFriendsLists){
            double maxScore = 0d;
            for(RecommendedUserBean recommendedUserBean: recommendedUserBeans){
                Double score = recommendedUserBean.getScore();
                if(score !=null){
                    maxScore = (maxScore < score) ? score : maxScore;
                }
            }
            recommendedUserBeans = normalise(recommendedUserBeans, maxScore);
            amalgamate(recommendedUserBeans, toReturn, noOfAlgs);
        }
        ArrayList<RecommendedUserBean> beans = new ArrayList<>(toReturn.values());

        return beans;
    }

    private void amalgamate(List<RecommendedUserBean> recommendedUserBeans, Map<String, RecommendedUserBean> current, int noOfAlgs) {
        for(RecommendedUserBean bean : recommendedUserBeans){
            RecommendedUserBean beanFromCurrent = current.get(bean.getUser());
            if(beanFromCurrent!=null){
                double score = (beanFromCurrent.getScore() * noOfAlgs + bean.getScore()) / noOfAlgs;
                beanFromCurrent.setScore(score);
            } else {
                bean.setScore(bean.getScore()/noOfAlgs);
                current.put(bean.getUser(), bean);
            }
        }
    }

    private List<RecommendedUserBean> normalise(List<RecommendedUserBean> recommendedUserBeans, double maxScore) {
        if(maxScore==0d) return recommendedUserBeans;
        for(RecommendedUserBean bean : recommendedUserBeans){
            bean.setScore(bean.getScore()/maxScore);
        }
        return recommendedUserBeans;
    }
}
