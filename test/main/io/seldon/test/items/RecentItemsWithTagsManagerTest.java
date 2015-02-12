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

package io.seldon.test.items;

import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.service.PersistenceProvider;
import io.seldon.general.jdo.SqlItemPeer;
import io.seldon.items.RecentItemsWithTagsManager;
import io.seldon.memcache.DogpileHandler;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;

public class RecentItemsWithTagsManagerTest {

	private SqlItemPeer mockItemPeer;
	private PersistenceProvider mockPP;
	private DogpileHandler mockDogpile;
	
	
	@Before
	public void setup()
	{
		mockItemPeer = createMock(SqlItemPeer.class);
		mockPP = createMock(PersistenceProvider.class);
		mockDogpile = createMock(DogpileHandler.class);
	}
	
	@Test 
	public void testNoItems() throws InterruptedException
	{
		final String client = "test";
		final int dimension = 1;
		final int attrId = 1;
		final int limit = 10;
		final String table = "varchar";
		final ConsumerBean c = new ConsumerBean(client);
		final Set<Long> ids = new HashSet<Long>();
		Map<Long,List<String>> res = new HashMap<Long,List<String>>();
		expect(mockItemPeer.getRecentItemTags(EasyMock.eq(ids), EasyMock.eq(attrId),EasyMock.eq(table))).andReturn(res);
		expect(mockPP.getItemPersister(client)).andReturn(mockItemPeer);
		replay(mockItemPeer);
		replay(mockPP);
		mockDogpile.updated((String) EasyMock.anyObject(), EasyMock.eq(RecentItemsWithTagsManager.CACHE_TIME_SECS));
		EasyMock.expectLastCall().once();
		replay(mockDogpile);
		
		RecentItemsWithTagsManager m = new RecentItemsWithTagsManager(mockPP,mockDogpile);
		Map<Long,List<String>> it = m.retrieveRecentItems(client, ids, attrId,table);
		Thread.sleep(1000);
		verify(mockItemPeer);
		verify(mockDogpile);
		assertEquals(0,it.size());
	}
	
	@Test 
	public void testItemsReturned() throws InterruptedException
	{
		final String client = "test";
		final int dimension = 1;
		final int attrId = 1;
		final int limit = 10;
		final Long itemId = 1L;
		final String table = "varchar";
		final ConsumerBean c = new ConsumerBean(client);
		Map<Long,List<String>> res = new HashMap<Long,List<String>>();
		List<String> tags = new ArrayList<String>();
		tags.add("tag");
		res.put(itemId, tags);
		final Set<Long> ids = new HashSet<Long>();
		ids.add(itemId);
		expect(mockItemPeer.getRecentItemTags(EasyMock.eq(ids), EasyMock.eq(attrId),EasyMock.eq(table))).andReturn(res).once();
		expect(mockPP.getItemPersister(client)).andReturn(mockItemPeer);
		replay(mockItemPeer);
		replay(mockPP);
		mockDogpile.updated((String) EasyMock.anyObject(), EasyMock.eq(RecentItemsWithTagsManager.CACHE_TIME_SECS));
		EasyMock.expectLastCall().once();
		expect(mockDogpile.updateIsRequired((String) EasyMock.anyObject(), EasyMock.eq(res), EasyMock.eq(RecentItemsWithTagsManager.CACHE_TIME_SECS))).andReturn(false);
		replay(mockDogpile);
		
		RecentItemsWithTagsManager m = new RecentItemsWithTagsManager(mockPP,mockDogpile);
		Map<Long,List<String>> it = m.retrieveRecentItems(client, ids,attrId,table);
		Thread.sleep(1000);
		Map<Long,List<String>> it2 = m.retrieveRecentItems(client, ids, attrId,table); // second call should return results
		verify(mockItemPeer);
		verify(mockDogpile);
		assertEquals(0,it.size());
		assertEquals(1,it2.size());
	}
	
	@Test 
	public void testItemsReturnedAndDogpileUpdateNeeded() throws InterruptedException
	{
		final String client = "test";
		final int dimension = 1;
		final int attrId = 1;
		final int limit = 10;
		final Long itemId1 = 1L;
		final Long itemId2 = 2L;
		final String table = "varchar";
		final ConsumerBean c = new ConsumerBean(client);
		Map<Long,List<String>> res = new HashMap<Long,List<String>>();
		List<String> tags = new ArrayList<String>();
		tags.add("tag");
		res.put(itemId1, tags);
		final Set<Long> ids = new HashSet<Long>();
		ids.add(itemId1);
		expect(mockItemPeer.getRecentItemTags(EasyMock.eq(ids), EasyMock.eq(attrId),EasyMock.eq(table))).andReturn(res).once();
		Map<Long,List<String>> res2 = new HashMap<Long,List<String>>();
		List<String> tags2 = new ArrayList<String>();
		tags.add("tag1");
		tags.add("tag2");
		res2.put(itemId1, tags);
		res2.put(itemId2, tags);
		final Set<Long> ids2 = new HashSet<Long>();
		ids2.add(itemId1);
		ids2.add(itemId2);
		expect(mockItemPeer.getRecentItemTags(EasyMock.eq(ids2), EasyMock.eq(attrId),EasyMock.eq(table))).andReturn(res2).once();
		expect(mockPP.getItemPersister(client)).andReturn(mockItemPeer).times(2);
		replay(mockItemPeer);
		replay(mockPP);
		mockDogpile.updated((String) EasyMock.anyObject(), EasyMock.eq(RecentItemsWithTagsManager.CACHE_TIME_SECS));
		EasyMock.expectLastCall().times(2);
		expect(mockDogpile.updateIsRequired((String) EasyMock.anyObject(), EasyMock.eq(res), EasyMock.eq(RecentItemsWithTagsManager.CACHE_TIME_SECS))).andReturn(true).once();
		expect(mockDogpile.updateIsRequired((String) EasyMock.anyObject(), EasyMock.eq(res), EasyMock.eq(RecentItemsWithTagsManager.CACHE_TIME_SECS))).andReturn(false).once();
		replay(mockDogpile);
		
		RecentItemsWithTagsManager m = new RecentItemsWithTagsManager(mockPP,mockDogpile);
		Map<Long,List<String>> it = m.retrieveRecentItems(client, ids,attrId,table);
		Thread.sleep(1000);
		Map<Long,List<String>> it2 = m.retrieveRecentItems(client, ids2, attrId,table); // second call should return results
		Thread.sleep(1000);
		Map<Long,List<String>> it3 = m.retrieveRecentItems(client, ids2, attrId,table); // second call should return results
		verify(mockItemPeer);
		verify(mockDogpile);
		assertEquals(0,it.size());
		assertEquals(1,it2.size());
		assertEquals(2,it3.size());
	}
	
}
