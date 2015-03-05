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
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.trust.impl.jdo.LastRecommendationBean;
import net.spy.memcached.MemcachedClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author firemanphil
 *         Date: 04/03/15
 *         Time: 15:30
 */
@Component
public class RecommendationStorage {

    private final MemcachedClient memcache;
    private final PersistenceProvider provider;
    private final DogpileHandler dogpileHandler;

    @Autowired
    public RecommendationStorage(PersistenceProvider provider, MemcachedClient memcache, DogpileHandler dogpileHandler) {
        this.memcache = memcache;
        this.provider = provider;
        this.dogpileHandler = dogpileHandler;
    }

    public LastRecommendationBean retrieveLastRecommendations(String client, String user, String recsCounter){
        int userRecCounter = Integer.parseInt(recsCounter);
        return (LastRecommendationBean) memcache.get(MemCacheKeys.getRecommendationListUUID(client, user, userRecCounter));
    }
}
