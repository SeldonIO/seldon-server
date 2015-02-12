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
import io.seldon.api.resource.service.InteractionService;
import io.seldon.general.Interaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Created by dylanlentini on 24/02/2014.
 */

@Component
public class InteractionsDecayFunction implements  SocialFriendsScoreDecayFunction
{

    @Autowired
    private InteractionService interactionService;


    @Override
    public List<RecommendedUserBean> decayFriendsScore(ConsumerBean client, UserBean user, List<RecommendedUserBean> friendsList)
    {
        Set<Interaction> interactionsList = interactionService.retrieveInteractions(client, user.getId(), InteractionService.MGM_TYPE);
        Set<String> decayFriendsList = new HashSet<String>();

        if (interactionsList != null)
        {
            for (Interaction interaction : interactionsList)
            {
                decayFriendsList.add(interaction.getUser2FbId());
            }
        }


        //function :: friends listed as interactions get their score set to 0.

        if (!decayFriendsList.isEmpty())
        {
            for (RecommendedUserBean friend : friendsList)
            {
                if (decayFriendsList.contains(friend.getUser()))
                {
                    friend.setScore(0.0);
                }
            }
        }

        return friendsList;
    }


}
















