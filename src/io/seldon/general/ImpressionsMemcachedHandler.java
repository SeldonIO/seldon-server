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

package io.seldon.general;


import io.seldon.api.resource.service.CacheExpireService;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


/**
 * Created by dylanlentini on 28/02/2014.
 */

@Component
public class ImpressionsMemcachedHandler implements ImpressionsPersistenceHandler {
    private static Logger logger = Logger.getLogger(ImpressionsMemcachedHandler.class.getName());
    private final String SERVICE_NAME = "MGM";
    @Autowired
    private CacheExpireService cacheExpireService;


    public ImpressionsMemcachedHandler(){}


    @Override
    public Impressions getImpressions(String client, String user)
    {
        String impressionsKey = MemCacheKeys.getFacebookUsersImpressionsKey("impressions", SERVICE_NAME, client, user);
        Impressions impressions = (Impressions) MemCachePeer.get(impressionsKey);
        logger.info("Retrieved impressions from memcache for client " + client + " and user " + user);
        return impressions;
    }


    @Override
    public void saveOrUpdateImpressions(String client, String user, Impressions impressions)
    {
        String impressionsKey = MemCacheKeys.getFacebookUsersImpressionsKey("impressions", SERVICE_NAME, client, user);
        MemCachePeer.put(impressionsKey, impressions, cacheExpireService.getCacheExpireSecs());
        logger.info("Saved impressions in memcache for client " + client + " and user " + user);
    }

    @Override
    public Impressions getPendingImpressions(String client, String user)
    {
        String impressionsKey = MemCacheKeys.getFacebookUsersImpressionsKey("pendingImpressions", SERVICE_NAME, client, user);
        Impressions impressions = (Impressions) MemCachePeer.get(impressionsKey);
        logger.info("Retrieved pending impressions from memcache for client " + client + " and user " + user);
        return impressions;
    }


    @Override
    public void saveOrUpdatePendingImpressions(String client, String user, Impressions impressions)
    {
        String impressionsKey = MemCacheKeys.getFacebookUsersImpressionsKey("pendingImpressions", SERVICE_NAME, client, user);
        MemCachePeer.put(impressionsKey, impressions, cacheExpireService.getCacheExpireSecs());
        logger.info("Saved pending impressions in memcache for client " + client + " and user " + user);
    }
}
