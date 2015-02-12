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

import io.seldon.api.resource.RecommendedUserBean;
import io.seldon.general.Impressions;
import io.seldon.general.ImpressionsPersistenceHandler;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Created by dylanlentini on 03/03/2014.
 */

@Service
public class ImpressionService {

    private static Logger logger = Logger.getLogger(ImpressionService.class.getName());
    public static final int MGM_TYPE = 1;

    @Autowired
    private PersistenceProvider persister;


    public ImpressionService(){}


    public void increaseImpressions(String client, String user, List<RecommendedUserBean> friends, int usersShown)
    {
        Impressions impressions = retrieveImpressions(client, user);

        if (impressions == null)
        {
            impressions = new Impressions(user, new HashMap<String, Integer>());
        }

        Map<String,Integer> decayFriendsList = impressions.getFriendsImpressions();


        for (int i=0; i<usersShown; i++)
        {
            if (friends.size() > i)
            {
                RecommendedUserBean friend = friends.get(i);
                if (decayFriendsList.containsKey(friend.getUser()))
                {
                    int impressionCount = decayFriendsList.get(friend.getUser());
                    decayFriendsList.put(friend.getUser(),impressionCount+1);
                }
                else
                {
                    decayFriendsList.put(friend.getUser(),1);
                }
            }
        }

        logger.info("Increasing impressions for shown friends for client " + client + " and user " + user);

        saveImpressions(client, user, impressions);

    }




    public void temporarilyStorePendingImpressions(String client, String user, List<RecommendedUserBean> friends, int usersShown)
    {

        Impressions impressions = new Impressions(user, new HashMap<String, Integer>());
        Map<String,Integer> decayFriendsList = impressions.getFriendsImpressions();

        for (int i=0; i<usersShown; i++)
        {
            if (friends.size() > i)
            {
                RecommendedUserBean friend = friends.get(i);
                decayFriendsList.put(friend.getUser(),1);
            }
        }

        logger.info("Temporarily setting impressions for to-be-shown friends for client " + client + " and user " + user);
        savePendingImpressions(client, user, impressions);

    }


    public void storeShownPendingImpressions(String client, String user)
    {
        Impressions pendingImpressions = retrievePendingImpressions(client, user);
        Impressions currentImpressions = retrieveImpressions(client, user);

        if (pendingImpressions == null)
        {
            logger.info("Pending impressions to be stored not found for client " + client + " and user " + user);
        }
        else
        {
            if (currentImpressions == null)
            {
                currentImpressions = new Impressions(user, new HashMap<String, Integer>());
            }

            Map<String,Integer> currentImpressionsMap = currentImpressions.getFriendsImpressions();
            Map<String,Integer> pendingImpressionsMap = pendingImpressions.getFriendsImpressions();

            for (Map.Entry<String,Integer> pendingImpression : pendingImpressionsMap.entrySet())
            {
                if (currentImpressionsMap.containsKey(pendingImpression.getKey()))
                {
                    int impressionCount = currentImpressionsMap.get(pendingImpression.getKey());
                    currentImpressionsMap.put(pendingImpression.getKey(),impressionCount+pendingImpression.getValue());
                }
                else
                {
                    currentImpressionsMap.put(pendingImpression.getKey(),pendingImpression.getValue());
                }
            }

            logger.info("Increasing impressions for shown ex-pending friends for client " + client + " and user " + user);
            saveImpressions(client, user, currentImpressions);
        }
    }



    public Impressions retrieveImpressions(String client, String user)
    {

        ImpressionsPersistenceHandler impressionsPersister = persister.getImpressionsPersister();
        Impressions impressions = impressionsPersister.getImpressions(client, user);

        return impressions;
    }

    public Impressions retrievePendingImpressions(String client, String user)
    {
        ImpressionsPersistenceHandler impressionsPersister = persister.getImpressionsPersister();
        Impressions pendingImpressions = impressionsPersister.getPendingImpressions(client, user);

        return pendingImpressions;
    }


    public void saveImpressions(String client, String user, Impressions impressions)
    {
        ImpressionsPersistenceHandler impressionsPersister = persister.getImpressionsPersister();
        impressionsPersister.saveOrUpdateImpressions(client, user, impressions);
    }


    public void savePendingImpressions(String client, String user, Impressions impressions)
    {
        ImpressionsPersistenceHandler impressionsPersister = persister.getImpressionsPersister();
        impressionsPersister.saveOrUpdatePendingImpressions(client, user, impressions);
    }



}
