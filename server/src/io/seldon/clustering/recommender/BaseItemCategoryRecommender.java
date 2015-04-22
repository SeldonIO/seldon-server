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

package io.seldon.clustering.recommender;

import io.seldon.api.Constants;
import io.seldon.api.Util;
import io.seldon.db.jdo.ClientPersistable;
import io.seldon.general.ItemPeer;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.recommendation.CFAlgorithm;

import org.apache.log4j.Logger;

/**
 * @author firemanphil
 *         Date: 10/12/14
 *         Time: 17:06
 */
public abstract class BaseItemCategoryRecommender {
    private static final String CATEGORY_DIM_OPT_NAME = "io.seldon.algorithm.clusters.categorydimensionname";
    private static Logger logger = Logger.getLogger(BaseItemCategoryRecommender.class.getName());

    protected Integer getDimensionForAttrName(long itemId, String client, RecommendationContext ctxt)
    {
        RecommendationContext.OptionsHolder opts = ctxt.getOptsHolder();
        ClientPersistable cp = new ClientPersistable(client);
        String attrName = opts.getStringOption(CATEGORY_DIM_OPT_NAME);
        String key = MemCacheKeys.getDimensionForAttrName(client, itemId, attrName);
        Integer dimId = (Integer) MemCachePeer.get(key);
        if (dimId == null)
        {
            ItemPeer iPeer = Util.getItemPeer(cp.getPM());
            dimId = iPeer.getDimensionForAttrName(itemId, attrName);
            if (dimId != null)
            {
                MemCachePeer.put(key, dimId, Constants.CACHING_TIME);
                logger.info("Get dim for item "+itemId+" for attrName "+attrName+" and got "+dimId);
            }
            else
                logger.info("Got null for dim for item "+itemId+" for attrName "+attrName);
        }
        return dimId;
    }
}
