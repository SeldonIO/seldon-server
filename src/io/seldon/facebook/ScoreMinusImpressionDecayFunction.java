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

import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.RecommendedUserBean;
import io.seldon.api.resource.UserBean;
import io.seldon.api.resource.service.ImpressionService;
import io.seldon.general.Impressions;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by dylanlentini on 13/02/2014.
 */


public class ScoreMinusImpressionDecayFunction implements SocialFriendsScoreDecayFunction
{
    @Autowired
    private ImpressionService impressionService;

    Double impressionDecayRate;


    public ScoreMinusImpressionDecayFunction(Double impressionDecayRate)
    {
        this.impressionDecayRate = impressionDecayRate;
    }



    @Override
    public List<RecommendedUserBean> decayFriendsScore(ConsumerBean client, UserBean user, List<RecommendedUserBean> friendsList)
    {
        Impressions impressions = impressionService.retrieveImpressions(client.getShort_name(), user.getId());

        if (impressions == null)
        {
            impressions = new Impressions(user.getId(), new HashMap<String, Integer>());
        }

        Map<String,Integer> decayFriendsList = impressions.getFriendsImpressions();
        Double decayedScore = 0.0;

        //function :: score - (impressionCount * rate)

        for (RecommendedUserBean friend : friendsList)
        {
            if (decayFriendsList.containsKey(friend.getUser()))
            {
                decayedScore = impressionDecayRate * decayFriendsList.get(friend.getUser());
                decayedScore = friend.getScore() - decayedScore;
                if (decayedScore > 0)
                {
                    friend.setScore(decayedScore);
                }
            }
        }


        return friendsList;
    }

}
