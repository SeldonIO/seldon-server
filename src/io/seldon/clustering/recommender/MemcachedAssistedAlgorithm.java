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

import java.util.*;

import io.seldon.trust.impl.ItemFilter;
import io.seldon.trust.impl.ItemIncluder;
import io.seldon.trust.impl.CFAlgorithm;
import net.spy.memcached.MemcachedClient;

import org.apache.log4j.Logger;

import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.memcache.SecurityHashPeer;

/**
 * @author firemanphil
 *         Date: 14/10/2014
 *         Time: 12:27
 */
public abstract class MemcachedAssistedAlgorithm extends BaseItemRecommendationAlgorithm {

    private static final int EXPIRY_TIME = 20 * 60;
    protected static Logger logger = Logger.getLogger(MemcachedAssistedAlgorithm.class.getName());

    public MemcachedAssistedAlgorithm(List<ItemIncluder> producers, List<ItemFilter> filters) {
        super(producers, filters);
    }


    @Override
    public ItemRecommendationResultSet recommend(CFAlgorithm options,String client, Long user, int dimensionId, int maxRecsCount,List<Long> recentitemInteractions) {
        if(user==null || client == null)
            return new ItemRecommendationResultSet(Collections.<ItemRecommendationResultSet.ItemRecommendationResult>emptyList());

        RecommendationContext ctxt = retrieveContext(client,dimensionId, options.getNumRecentItems());
        ItemRecommendationResultSet res;
        try {
            res = (ItemRecommendationResultSet) getMemcache().get(getCacheKey(client, user, dimensionId));
        } catch (Exception e){
            logger.error("Problem when retrieving memcached results for key "+getCacheKey(client, user, dimensionId),e);
            res= null;
        }

        res = filter(res, ctxt);
        if(res!=null && res.getResults().size() >= maxRecsCount) {
            logger.debug("Found sufficient recommendations in memcache returning " + res.getResults().size() + " recs.");
            return res;
        } else
            return filter(obtainNewRecommendations(options,client, user, dimensionId, ctxt, maxRecsCount, recentitemInteractions),ctxt);



    }


    private ItemRecommendationResultSet obtainNewRecommendations(CFAlgorithm options,String client, Long user, int dimensionId,
            RecommendationContext ctxt, int maxRecsCount, List<Long> recentitemInteractions) {
        ItemRecommendationResultSet set =  recommendWithoutCache(options,client, user, dimensionId, ctxt,maxRecsCount, recentitemInteractions);
        try{
            getMemcache().set(getCacheKey(client, user, dimensionId),EXPIRY_TIME,set);
        } catch (Exception e) {
            logger.error("Problem when saving memcached results for key " + getCacheKey(client, user, dimensionId), e);
        }
        return set;
    }

    public MemcachedClient getMemcache(){
        return MemCachePeer.getClient();
    }

    private String getCacheKey(String client, Long user, int dimension) {
        return SecurityHashPeer.md5digest(MemCacheKeys.getRecommendedItemsPerAlgKey(client, this.getClass().getSimpleName(),user, dimension));
    }


    public abstract ItemRecommendationResultSet recommendWithoutCache(CFAlgorithm options,
            String client, Long user, int dimension, RecommendationContext ctxt, int maxRecsCount, List<Long> recentitemInteractions);


    public ItemRecommendationResultSet filter(ItemRecommendationResultSet unfiltered, RecommendationContext ctxt){
        if(unfiltered==null) return null;
        List<ItemRecommendationResultSet.ItemRecommendationResult> resultSet = unfiltered.getResults();
        Iterator<ItemRecommendationResultSet.ItemRecommendationResult> iter = resultSet.iterator();
        while (iter.hasNext()){
            ItemRecommendationResultSet.ItemRecommendationResult result = iter.next();
            if((ctxt.mode== RecommendationContext.MODE.EXCLUSION && ctxt.contextItems.contains(result.item))
                    || (ctxt.mode== RecommendationContext.MODE.INCLUSION && !ctxt.contextItems.contains(result.item))){
                iter.remove();
            }
        }
        return new ItemRecommendationResultSet(resultSet);
    }
}
