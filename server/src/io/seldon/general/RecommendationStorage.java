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

import io.seldon.api.resource.service.PersistenceProvider;
import io.seldon.memcache.DogpileHandler;
import io.seldon.memcache.ExceptionSwallowingMemcachedClient;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.trust.impl.jdo.LastRecommendationBean;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.apache.log4j.Logger;

/**
 * @author firemanphil
 *         Date: 04/03/15
 *         Time: 15:30
 */
@Component
public class RecommendationStorage {

    private static final Logger logger = Logger.getLogger(RecommendationStorage.class.getName());
    private final PersistenceProvider provider;
    private final DogpileHandler dogpileHandler;
    private final ExceptionSwallowingMemcachedClient memcachedClient;

    @Autowired
    public RecommendationStorage(PersistenceProvider provider, ExceptionSwallowingMemcachedClient memcachedClient, DogpileHandler dogpileHandler) {
        this.provider = provider;
        this.memcachedClient = memcachedClient;
        this.dogpileHandler = dogpileHandler;
    }

    public LastRecommendationBean retrieveLastRecommendations(String client, String user, String recsCounter){
        int userRecCounter = Integer.parseInt(recsCounter.trim());
        LastRecommendationBean lastRecommendations = (LastRecommendationBean) memcachedClient.get(MemCacheKeys.getRecommendationListUUID(client, user, userRecCounter));
        return lastRecommendations;
    }
}
