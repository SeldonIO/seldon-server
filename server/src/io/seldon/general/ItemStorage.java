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
import io.seldon.general.jdo.SqlItemPeer;
import io.seldon.memcache.DogpileHandler;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.memcache.SecurityHashPeer;
import io.seldon.memcache.UpdateRetriever;
import io.seldon.recommendation.filters.FilteredItems;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author firemanphil
 *         Date: 18/11/14
 *         Time: 11:49
 */
@Component
public class ItemStorage {

    private static Logger logger = Logger.getLogger( ItemStorage.class.getName() );

    // 5 mins
    private static final int MOST_POPULAR_EXPIRE_TIME = 5 * 60;
    // 15 mins
    private static final int RECENT_ITEMS_EXPIRE_TIME = 15 * 60;
    private static final int MEMCACHE_EXCLUSIONS_EXPIRE_SECS = 30 * 60;
    private final PersistenceProvider provider;
    private final DogpileHandler dogpileHandler;

    @Autowired
    public ItemStorage(PersistenceProvider provider, DogpileHandler dogpileHandler) {
        this.provider = provider;
        this.dogpileHandler = dogpileHandler;
    }

    private String getMostPopularCacheKey(String client,Set<Integer> dimensions, int numItems)
    {
    	return MemCacheKeys.getPopularItems(client, dimensions, numItems);
    }
    
    public List<SqlItemPeer.ItemAndScore> retrieveMostPopularItemsWithScore(final String client, final int numItems, final Set<Integer> dimensions){
    	return retrieveMostPopularItemsWithScoreImpl(getMostPopularCacheKey(client, dimensions, numItems), client, numItems, dimensions);
    }
    
    private List<SqlItemPeer.ItemAndScore> retrieveMostPopularItemsWithScoreImpl(final String key,final String client, final int numItems, final Set<Integer> dimensions){

        
        List<SqlItemPeer.ItemAndScore> retrievedItems = retrieveUsingJSON(key, numItems,
                new UpdateRetriever<List<SqlItemPeer.ItemAndScore>>() {
                    @Override
                    public List<SqlItemPeer.ItemAndScore> retrieve() throws Exception {
                        return provider.getItemPersister(client).retrieveMostPopularItems(numItems, dimensions);
                    }
                }, new TypeReference<List<SqlItemPeer.ItemAndScore>>() {}, MOST_POPULAR_EXPIRE_TIME);

        return retrievedItems==null? Collections.EMPTY_LIST: retrievedItems;
    }

    public FilteredItems retrieveMostPopularItems(final String client, final int numItems, final Set<Integer> dimensions){
    	final String key = getMostPopularCacheKey(client, dimensions, numItems);
    	List<SqlItemPeer.ItemAndScore> retrievedItems = retrieveMostPopularItemsWithScoreImpl(key, client, numItems, dimensions);
    	List<Long> toReturn = new ArrayList<>();
        for (SqlItemPeer.ItemAndScore itemAndScore : retrievedItems){
            toReturn.add(itemAndScore.item);
        }
        return new FilteredItems(toReturn.size() >= numItems ? new ArrayList<>(toReturn).subList(0,numItems) : toReturn, SecurityHashPeer.md5(key));
    }
    
    public FilteredItems retrieveRecentlyAddedItems(final String client, final int numItems, final Set<Integer> dimensions){
        final String key = MemCacheKeys.getRecentItems(client, dimensions, numItems);
        List<Long> retrievedItems = retrieveUsingJSON(key, numItems, new UpdateRetriever<List<Long>>() {
            @Override
            public List<Long> retrieve() throws Exception {
                return provider.getItemPersister(client).getRecentItemIds(dimensions, numItems, null);
            }
        }, new TypeReference<List<Long>>() {}, RECENT_ITEMS_EXPIRE_TIME);
        return new FilteredItems(retrievedItems==null? Collections.EMPTY_LIST : retrievedItems,SecurityHashPeer.md5(key));
    }
    
    public FilteredItems retrieveExplicitItems(final String client,final Set<Long> items)
    
    {
    	final String key = MemCacheKeys.getExplicitItemsIncluderKey(client, items);
    	List<Long> retrievedItems = retrieveUsingJSON(key, items.size(), new UpdateRetriever<List<Long>>() {
            @Override
            public List<Long> retrieve() throws Exception {
                return new ArrayList<Long>(items);
            }
        }, new TypeReference<List<Long>>() {}, RECENT_ITEMS_EXPIRE_TIME);
        return new FilteredItems(retrievedItems==null? Collections.EMPTY_LIST : retrievedItems,SecurityHashPeer.md5(key));
    }

    private <T extends List> T retrieveUsingJSON(String key, int numItemsRequired, UpdateRetriever<T> retriever,
                                                 TypeReference<T> typeRetriever,int expireTime) {
    	final ObjectMapper mapper = new ObjectMapper();
    	T retrievedItems = null;
    	String json = (String) MemCachePeer.get(key);
    	if (json != null)
    	{
    		try 
    		{
    			retrievedItems = mapper.readValue(json,typeRetriever);
    		} catch (Exception e1) {
    			logger.error("Failed to parae json "+json,e1);
    		}
    	}
        if (retrievedItems==null || retrievedItems.size() < numItemsRequired) retrievedItems = null;
        T newerRetrievedItems = null;
        try {
            newerRetrievedItems = dogpileHandler.retrieveUpdateIfRequired(key, retrievedItems, retriever, expireTime);
            if(newerRetrievedItems!=null){
            	String result = mapper.writeValueAsString(newerRetrievedItems);
                MemCachePeer.put(key, result, expireTime);
                return newerRetrievedItems;
            }
        } catch (Exception e) {
            logger.warn("Couldn't retrieve items when using dogpile handler" , e);
        }
        return retrievedItems;
    }

    public Set<Long> retrieveIgnoredItems(String client, String clientUserId) {
        // no dogpile handler as this will change regularly and we aren't using the DB

        final String exKey = MemCacheKeys.getExcludedItemsForRecommendations(client, clientUserId);
        Set<Long> ignored =  (Set<Long>) MemCachePeer.get(exKey);
        return ignored==null? Collections.<Long>emptySet() : ignored;
    }

    public void persistIgnoredItems(String client, String clientUserId, Set<Long> ignoredItems){

        final String exKey = MemCacheKeys.getExcludedItemsForRecommendations(client, clientUserId);
        MemCachePeer.put(exKey, ignoredItems, MEMCACHE_EXCLUSIONS_EXPIRE_SECS);
    }
}
