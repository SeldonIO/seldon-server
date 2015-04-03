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

package io.seldon.api.caching;

import io.seldon.api.APIException;
import io.seldon.api.state.NewClientListener;
import io.seldon.api.state.options.DefaultOptions;
import io.seldon.api.state.zk.ZkClientConfigHandler;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import net.spy.memcached.CASMutation;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ActionHistoryCache implements NewClientListener {

	private static Logger logger = Logger.getLogger(ActionHistoryCache.class.getName());
	public static int CACHE_TIME = 1800;
	
	private static ConcurrentHashMap<String,Boolean> clientsUseDb = new ConcurrentHashMap<>();
	
	DefaultOptions options;
	ZkClientConfigHandler clientConfigHandler;
	
	@Autowired
	public ActionHistoryCache(DefaultOptions options,ZkClientConfigHandler clientConfigHandler)
	{
		this.options = options;
		this.clientConfigHandler = clientConfigHandler;
		clientConfigHandler.addNewClientListener(this, true);
	}
	
	@Override
	public void clientAdded(String client) {
	}

	@Override
	public void clientDeleted(String client) {
	}
	
	
	
	public void removeRecentActions(String clientName,long userId)
	{
		String mkey = MemCacheKeys.getActionHistory(clientName, userId);
		MemCachePeer.delete(mkey);
	}
		
	public List<Long> getRecentActions(String clientName,long userId,int numActions)
	{
		String mkey = MemCacheKeys.getActionHistory(clientName, userId);
		List<Long> res = (List<Long>) MemCachePeer.get(mkey);
		if (res == null)
		{
			logger.debug("creating empty action history for user "+userId+" for client "+clientName);
			res = new ArrayList<>();
		}
		else 
		{
			if (res.size() > numActions)
				res = res.subList(0, numActions);
			logger.debug("Got action history for user "+userId+" from memcache");
		}
		return res;
	}
	
	public void addAction(String clientName,long userId,final long itemId) throws APIException
    {
		logger.debug("Adding action to cache for "+userId+" item "+itemId);
        CASMutation<List<Long>> mutation = new CASMutation<List<Long>>() {

            // This is only invoked when a value actually exists.
            public List<Long> getNewValue(List<Long> current) {
            	if(!current.contains(itemId)) {
	                current.add(0, itemId);
            	}
	            return current;
            }
        };
        List<Long> actions = new ArrayList<>();
        actions.add(itemId);
        String mkey = MemCacheKeys.getActionHistory(clientName, userId);
        MemCachePeer.cas(mkey, mutation, actions,CACHE_TIME);
    }
	
	
}
