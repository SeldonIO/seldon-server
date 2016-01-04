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
import io.seldon.api.caching.ActionHistory;
import io.seldon.api.caching.ActionHistoryProvider;
import io.seldon.api.caching.redis.RedisActionHistory;
import io.seldon.api.state.ClientConfigHandler;
import io.seldon.api.state.ClientConfigUpdateListener;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;

public class ActionHistoryProviderTest {

	private ClientConfigHandler mockClientConfigHandler;
	private ApplicationContext mockApplicationContext;
	private RedisActionHistory mockRedisActionHistory;
	
	@Before
	public void createMocks()
	{
		this.mockClientConfigHandler =  createMock(ClientConfigHandler.class);
		this.mockApplicationContext = createMock(ApplicationContext.class);
		this.mockRedisActionHistory = createMock(RedisActionHistory.class);
	}
	
	@Test
	public void test_add_actions_is_not_run()
	{
		mockClientConfigHandler.addListener((ClientConfigUpdateListener) EasyMock.anyObject());
		EasyMock.expectLastCall().once();
		replay(mockClientConfigHandler);
		
		Map<String,ActionHistory> beans = new HashMap<String,ActionHistory>();
		
		Capture<Class<?>> classCapture1 = new Capture<Class<?>>();
		Capture<Class<?>> classCapture2 = new Capture<Class<?>>();
		mockApplicationContext.getBeansOfType(EasyMock.capture(classCapture1));
		EasyMock.expectLastCall().andReturn(beans).once();
		mockApplicationContext.getBean((String)EasyMock.anyObject(),EasyMock.capture(classCapture2));
		EasyMock.expectLastCall().andReturn(mockRedisActionHistory).once();
		replay(mockApplicationContext);
		
		ActionHistoryProvider p = new ActionHistoryProvider(mockClientConfigHandler);
		verify(mockClientConfigHandler);
		
		replay(mockRedisActionHistory);
		
		p.setApplicationContext(mockApplicationContext);
		
		final String client = "client1";
		p.configUpdated(client, ActionHistoryProvider.ACTION_HISTORY_KEY, "{\"type\":\"redisActionHistory\",\"addActions\":false}");
		
		p.addAction(client, 1L, 1l);
		
		
		verify(mockApplicationContext);
		verify(mockRedisActionHistory);
		Assert.assertEquals(classCapture1.getValue(), ActionHistory.class);
		Assert.assertEquals(classCapture2.getValue(), ActionHistory.class);
		
	}
	
	@Test
	public void test_add_actions_is_run()
	{
		mockClientConfigHandler.addListener((ClientConfigUpdateListener) EasyMock.anyObject());
		EasyMock.expectLastCall().once();
		replay(mockClientConfigHandler);
		
		Map<String,ActionHistory> beans = new HashMap<String,ActionHistory>();
		
		Capture<Class<?>> classCapture1 = new Capture<Class<?>>();
		Capture<Class<?>> classCapture2 = new Capture<Class<?>>();
		mockApplicationContext.getBeansOfType(EasyMock.capture(classCapture1));
		EasyMock.expectLastCall().andReturn(beans).once();
		mockApplicationContext.getBean((String)EasyMock.anyObject(),EasyMock.capture(classCapture2));
		EasyMock.expectLastCall().andReturn(mockRedisActionHistory).once();
		replay(mockApplicationContext);
		
		ActionHistoryProvider p = new ActionHistoryProvider(mockClientConfigHandler);
		verify(mockClientConfigHandler);
		
		mockRedisActionHistory.addAction((String) EasyMock.anyObject(), EasyMock.anyLong(), EasyMock.anyLong());
		EasyMock.expectLastCall().once();
		replay(mockRedisActionHistory);
		
		p.setApplicationContext(mockApplicationContext);
		
		final String client = "client1";
		p.configUpdated(client, ActionHistoryProvider.ACTION_HISTORY_KEY, "{\"type\":\"redisActionHistory\",\"addActions\":true}");
		
		p.addAction(client, 1L, 1l);
		
		
		verify(mockApplicationContext);
		verify(mockRedisActionHistory);
		Assert.assertEquals(classCapture1.getValue(), ActionHistory.class);
		Assert.assertEquals(classCapture2.getValue(), ActionHistory.class);
		
	}
	
	@Test
	public void test_partial_config_add_actions_is_run()
	{
		mockClientConfigHandler.addListener((ClientConfigUpdateListener) EasyMock.anyObject());
		EasyMock.expectLastCall().once();
		replay(mockClientConfigHandler);
		
		Map<String,ActionHistory> beans = new HashMap<String,ActionHistory>();
		
		Capture<Class<?>> classCapture1 = new Capture<Class<?>>();
		Capture<Class<?>> classCapture2 = new Capture<Class<?>>();
		mockApplicationContext.getBeansOfType(EasyMock.capture(classCapture1));
		EasyMock.expectLastCall().andReturn(beans).once();
		mockApplicationContext.getBean((String)EasyMock.anyObject(),EasyMock.capture(classCapture2));
		EasyMock.expectLastCall().andReturn(mockRedisActionHistory).once();
		replay(mockApplicationContext);
		
		ActionHistoryProvider p = new ActionHistoryProvider(mockClientConfigHandler);
		verify(mockClientConfigHandler);
		
		mockRedisActionHistory.addAction((String) EasyMock.anyObject(), EasyMock.anyLong(), EasyMock.anyLong());
		EasyMock.expectLastCall().once();
		replay(mockRedisActionHistory);
		
		p.setApplicationContext(mockApplicationContext);
		
		final String client = "client1";
		p.configUpdated(client, ActionHistoryProvider.ACTION_HISTORY_KEY, "{\"type\":\"redisActionHistory\"}");
		
		p.addAction(client, 1L, 1l);
		
		
		verify(mockApplicationContext);
		verify(mockRedisActionHistory);
		Assert.assertEquals(classCapture1.getValue(), ActionHistory.class);
		Assert.assertEquals(classCapture2.getValue(), ActionHistory.class);
		
	}
}
