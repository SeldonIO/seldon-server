/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.api.caching;

import io.seldon.api.APIException;
import io.seldon.general.Action;
import io.seldon.memcache.ExceptionSwallowingMemcachedClient;
import io.seldon.memcache.MemCacheKeys;

import java.util.ArrayList;
import java.util.List;

import net.spy.memcached.CASMutation;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MemcacheActionHistory implements ActionHistory {

	private static Logger logger = Logger.getLogger(MemcacheActionHistory.class.getName());
	public static int CACHE_TIME = 1800;
	
	@Autowired
	ExceptionSwallowingMemcachedClient cacheClient;
	
	@Override
	public List<Long> getRecentActions(String clientName,long userId,int numActions)
	{
		String mkey = MemCacheKeys.getActionHistory(clientName, userId);
		List<Long> res = (List<Long>) cacheClient.get(mkey);
		if (res == null)
		{
			if (logger.isDebugEnabled())
				logger.debug("creating empty action history for user "+userId+" for client "+clientName);
			res = new ArrayList<>();
		}
		else 
		{
			if (res.size() > numActions)
				res = res.subList(0, numActions);
			if (logger.isDebugEnabled())
				logger.debug("Got action history for user "+userId+" from memcache");
		}
		return res;
	}

	@Override
	public List<Action> getRecentFullActions(String clientName,long userId,int numActions)
	{
		String mkey = MemCacheKeys.getActionFullHistory(clientName, userId);
		List<Action> res = (List<Action>) cacheClient.get(mkey);
		if (res == null)
		{
			if (logger.isDebugEnabled())
				logger.debug("creating empty action full history for user "+userId+" for client "+clientName);
			res = new ArrayList<Action>();
		}
		else 
		{
			if (logger.isDebugEnabled())
				logger.debug("Got action full history for user "+userId+" from memcache of size " + res.size());
			if (res.size() > numActions)
				res = res.subList(0, numActions);

		}
		return res;
	}
	
	@Override
	public void addFullAction(String clientName,final Action a)
	{
		if (logger.isDebugEnabled())
			logger.debug("Adding full action to cache for "+a.getUserId()+" item "+a.getItemId());
        CASMutation<List<Action>> mutation = new CASMutation<List<Action>>() {

            // This is only invoked when a value actually exists.
            public List<Action> getNewValue(List<Action> current) {
	                current.add(0, a);
	            return current;
            }
        };
        List<Action> actions = new ArrayList<Action>();
        actions.add(a);
		String mkey = MemCacheKeys.getActionFullHistory(clientName, a.getUserId());
        cacheClient.cas(mkey, mutation, actions,CACHE_TIME);
		
	}
	
	@Override
	public void addAction(String clientName,long userId,final long itemId) throws APIException
    {
		if (logger.isDebugEnabled())
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
        cacheClient.cas(mkey, mutation, actions,CACHE_TIME);
    }
	
	
	
	

}
