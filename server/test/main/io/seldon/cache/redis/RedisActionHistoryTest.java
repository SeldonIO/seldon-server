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
package io.seldon.cache.redis;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import io.seldon.api.caching.redis.RedisActionHistory;
import io.seldon.db.redis.RedisPoolManager;
import io.seldon.general.Action;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisActionHistoryTest {

	private RedisPoolManager mockRedisPoolManager;
	private JedisPool mockJedisPool;
	private Jedis mockJedis;
	
	@Before
	public void createMocks()
	{
		this.mockRedisPoolManager = createMock(RedisPoolManager.class);
		this.mockJedisPool = createMock(JedisPool.class);
		this.mockJedis = createMock(Jedis.class);
	}

	@Test
	public void test_get_full_action()
	{
		mockRedisPoolManager.get((String) EasyMock.anyObject());
		EasyMock.expectLastCall().andReturn(mockJedisPool).once();
		replay(mockRedisPoolManager);
		
		String client = "client1";
		
		String redisActionEntry = "{\"client\":\"testclient\",\"rectag\":\"default\",\"userid\":1,\"itemid\":3,\"type\":1,\"value\":\"0.0\",\"client_userid\":\"test_user1\",\"client_itemid\":\"item2\"}";
		Set<String> res = new LinkedHashSet<String>();
		res.add(redisActionEntry);
		
		mockJedisPool.getResource();
		EasyMock.expectLastCall().andReturn(mockJedis).once();
		replay(mockJedisPool);
		
		mockJedis.zrange((String) EasyMock.anyObject(),EasyMock.anyInt(),EasyMock.anyInt());
		EasyMock.expectLastCall().andReturn(res).once();
		mockJedis.close();
		EasyMock.expectLastCall().once();
		replay(mockJedis);
		
		RedisActionHistory rah = new RedisActionHistory(mockRedisPoolManager);
		List<Action> actions = rah.getRecentFullActions(client, 1L, 10);
		Assert.assertEquals(1, actions.size());
		Action a = actions.get(0);
		Assert.assertEquals(1L, a.getUserId());
		Assert.assertEquals(3L, a.getItemId());
		verify(mockJedis);
		
	}
	
	@Test
	public void test_get_actions()
	{
		mockRedisPoolManager.get((String) EasyMock.anyObject());
		EasyMock.expectLastCall().andReturn(mockJedisPool).once();
		replay(mockRedisPoolManager);
		
		mockJedisPool.getResource();
		EasyMock.expectLastCall().andReturn(mockJedis).once();
		replay(mockJedisPool);
		
		Set<String> res = new LinkedHashSet<String>();
		res.add("1");
		res.add("2");
		
		mockJedis.zrange((String) EasyMock.anyObject(),EasyMock.anyInt(),EasyMock.anyInt());
		EasyMock.expectLastCall().andReturn(res).once();
		mockJedis.close();
		EasyMock.expectLastCall().once();
		replay(mockJedis);
		
		RedisActionHistory rah = new RedisActionHistory(mockRedisPoolManager);
		List<Long> actions = rah.getRecentActions("client", 1L, 1);
		Assert.assertTrue(actions.size() == 1);
		Assert.assertEquals(2L, (long)actions.get(0));
		//Assert.assertEquals(1L, (long)actions.get(1));
		verify(mockJedis);
		
	}
	
}
