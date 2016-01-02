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
package io.seldon.api.caching.redis;

import io.seldon.api.APIException;
import io.seldon.api.caching.ActionHistory;
import io.seldon.api.caching.ActionLogEntry;
import io.seldon.db.redis.RedisPoolManager;
import io.seldon.general.Action;
import io.seldon.memcache.MemCacheKeys;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class RedisActionHistory implements ActionHistory {
	private static Logger logger = Logger.getLogger(RedisActionHistory.class.getName());
	
	final private RedisPoolManager poolManager;
	private ObjectMapper mapper = new ObjectMapper();
	
	@Autowired
	public RedisActionHistory(RedisPoolManager poolManager)
	{
		this.poolManager = poolManager;
	}
	
	
	
	@Override
	public List<Long> getRecentActions(String clientName, long userId,int numActions) {
		List<Long> res = new ArrayList<Long>();
		JedisPool pool = poolManager.get(clientName);
		if (pool != null)
		{
			Jedis jedis = null;
			try
			{
				jedis = pool.getResource();
				String key = MemCacheKeys.getActionHistory(clientName, userId);
				Set<String> itemSet = jedis.zrange(key, 0, -1);
				for (String item : itemSet)
					res.add(Long.parseLong(item));
			}
			finally
			{
			 if (jedis != null) {
				    jedis.close();
				  }
			}
		}
		else
		{
			logger.error("No redis pool found for "+clientName);
		}
		if (logger.isDebugEnabled())
			logger.debug("For user "+userId+" found "+res.size()+" actions:"+res.toString());
		return res;
	}

	@Override
	public List<Action> getRecentFullActions(String clientName, long userId,int numActions) {
		List<Action> actions = new ArrayList<Action>();
		JedisPool pool = poolManager.get(clientName);
		if (pool != null)
		{
			Jedis jedis = null;
			try
			{
				jedis = pool.getResource();
				String key = MemCacheKeys.getActionFullHistory(clientName, userId);
				Set<String> actionSet = jedis.zrange(key, 0, -1);
				for (String val : actionSet)
				{
	                ActionLogEntry ale = mapper.readValue(val, ActionLogEntry.class);
	                actions.add(ale.toAction());
				}
			}
			catch (IOException e) {
				logger.error("Failed to convert values to actions ",e);
			}
			finally
			{
			 if (jedis != null) {
				    jedis.close();
				  }
			}
		}
		else
		{
			logger.error("No redis pool found for "+clientName);
		}
		if (logger.isDebugEnabled())
			logger.debug("For user "+userId+" found "+actions.size()+" full actions");
		return actions;
	}

	@Override
	public void addFullAction(String clientName, Action a) throws APIException {

		JedisPool pool = poolManager.get(clientName);
		if (pool != null)
		{
			Jedis jedis = null;
			try 
			{
				ActionLogEntry al = new ActionLogEntry(clientName,a);
				String val = mapper.writeValueAsString(al);
				String key = MemCacheKeys.getActionFullHistory(clientName, a.getUserId());
				jedis = pool.getResource();
				jedis.zadd(key, a.getDate().getTime()/1000, val);
				//zremrangebyscore needed
			}
			catch (JsonProcessingException e) 
			{
				logger.error("Failed to convert action to json ",e);
			}
			finally
			{
				if (jedis != null)
				{
					jedis.close();
				}
			}
		}
		else
		{
			logger.error("No redis pool found for "+clientName);
		}
	}

	@Override
	public void addAction(String clientName, long userId, long itemId)
			throws APIException {
		JedisPool pool = poolManager.get(clientName);
		if (pool != null)
		{
			Jedis jedis = null;
			try 
			{
				long now = System.currentTimeMillis()/1000;
				String key = MemCacheKeys.getActionHistory(clientName, userId);
				jedis = pool.getResource();
				jedis.zadd(key, now, ""+itemId);
				//zremrangebyscore needed
			}
			finally
			{
				if (jedis != null)
				{
					jedis.close();
				}
			}
		}
		else
		{
			logger.error("No redis pool found for "+clientName);
		}
		
	}
	
	

}
