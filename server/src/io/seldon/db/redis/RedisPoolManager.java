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
package io.seldon.db.redis;

import io.seldon.api.state.ClientConfigHandler;
import io.seldon.api.state.ClientConfigUpdateListener;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class RedisPoolManager implements ClientConfigUpdateListener {
	private static Logger logger = Logger.getLogger(RedisPoolManager.class.getName());

	ConcurrentMap<String,JedisPool> pools = new ConcurrentHashMap<String,JedisPool>();
	public static final String REDIS_KEY = "redis";
	
	@Autowired
	public RedisPoolManager(ClientConfigHandler configHandler)
	{
		configHandler.addListener(this);
	}
	
	private void add(String client,String host)
	{
		logger.info("Adding Redis pool for "+client+" at "+host);
		JedisPool pool = new JedisPool(new JedisPoolConfig(), host);
		JedisPool existing = pools.get(client);
		pools.put(client, pool);
		if (existing != null)
		{
			logger.warn("Attempting to close previous pool for "+client);
			existing.destroy();
		}
	}
	
	private void remove(String client)
	{
		logger.info("Removing pool for "+client);
		JedisPool pool = pools.remove(client);
		if (pool != null)
		{
			pool.destroy();
		}
	}
	
	public JedisPool get(String client)
	{
		return pools.get(client);
	}

	@Override
	public void configUpdated(String client, String configKey,
			String configValue) {
		if (configKey.equals(REDIS_KEY)){
			logger.info("Received new redis config for "+ client+": "+ configValue);
			try {
				ObjectMapper mapper = new ObjectMapper();
				RedisConfig config = mapper.readValue(configValue, RedisConfig.class);
				add(client,config.host);
				logger.info("Successfully added new redis config for "+client);
	            } catch (IOException | BeansException e) {
	                logger.error("Couldn't update redis for client " +client, e);
	            }
		}
		
	}
	
	public static class RedisConfig
	{
		public String host;
	}

	@Override
	public void configRemoved(String client, String configKey) {
		if (configKey.equals(REDIS_KEY)){
			remove(client);
		}
	}
	
}
