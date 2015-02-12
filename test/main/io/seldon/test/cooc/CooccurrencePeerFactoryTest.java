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

package io.seldon.test.cooc;

import java.util.Properties;

import io.seldon.cooc.CooccurrencePeer;
import io.seldon.cooc.CooccurrencePeerFactory;
import junit.framework.Assert;

import org.junit.Test;

public class CooccurrencePeerFactoryTest {

	@Test
	public void propsTest()
	{
		Properties p = new Properties();
		final String client = "TEST";
		int staleSec = 1234;
		int cacheSize = 5678;
		p.setProperty(CooccurrencePeerFactory.PROP_PREFIX+"clients", client);
		p.setProperty(CooccurrencePeerFactory.PROP_PREFIX+client+CooccurrencePeerFactory.STALE_SECS_SUFFIX, ""+staleSec);
		p.setProperty(CooccurrencePeerFactory.PROP_PREFIX+client+CooccurrencePeerFactory.CACHE_SIZE_SUFFIX, ""+cacheSize);
		
		CooccurrencePeerFactory.initialise(p);
		
		CooccurrencePeer peer = CooccurrencePeerFactory.get(client);
		
		Assert.assertEquals(staleSec, peer.getStaleCountTimeSecs());
		Assert.assertEquals(cacheSize, peer.getCacheSize());
		Assert.assertEquals(client, peer.getClient());
	}
	
	@Test 
	public void testDefaultPeer()
	{
		String client = "abcd";
		CooccurrencePeer peer = CooccurrencePeerFactory.get(client);
		
		Assert.assertEquals(CooccurrencePeerFactory.DEF_STALE_TIME_SECS, peer.getStaleCountTimeSecs());
		Assert.assertEquals(CooccurrencePeerFactory.DEF_CACHE_SIZE, peer.getCacheSize());
		Assert.assertEquals(client, peer.getClient());
	}
	
}
