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
package io.seldon.redis;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import io.seldon.api.state.ClientConfigHandler;
import io.seldon.api.state.ClientConfigUpdateListener;
import io.seldon.db.redis.RedisPoolManager;
import junit.framework.Assert;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.JedisPool;

public class RedisPoolManagerTest {

	private ClientConfigHandler mockClientConfigHandler;
	
	@Before
	public void createMocks()
	{
		this.mockClientConfigHandler =  createMock(ClientConfigHandler.class);
	}
	
	@Test
	public void test_add()
	{
		mockClientConfigHandler.addListener((ClientConfigUpdateListener) EasyMock.anyObject());
		EasyMock.expectLastCall().once();
		replay(mockClientConfigHandler);
		
		RedisPoolManager poolManager = new RedisPoolManager(mockClientConfigHandler);
		
		final String client = "client1";
		poolManager.configUpdated(client, RedisPoolManager.REDIS_KEY, "{\"host\":\"localhost\"}");
		
		JedisPool jp = poolManager.get(client);
		Assert.assertNotNull(jp);
		
	}
}
