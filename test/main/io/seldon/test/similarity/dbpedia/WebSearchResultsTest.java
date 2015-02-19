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

package io.seldon.test.similarity.dbpedia;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.seldon.api.resource.ItemBean;
import io.seldon.similarity.dbpedia.DBpediaItemSearch;
import io.seldon.similarity.dbpedia.jdo.SqlWebSearchResultsPeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.general.Item;
import io.seldon.similarity.dbpedia.WebSearchResultsPeer;

@Ignore
public class WebSearchResultsTest extends BasePeerTest {
	
	@Autowired
	GenericPropertyHolder props;
	
	WebSearchResultsPeer webSearchResultsPeer;
	
	@Before
	public void setUp()
	{
		webSearchResultsPeer = new SqlWebSearchResultsPeer(props.getClient());
	}
	
	 @Test	
	 public void testGetHits()
	 {
		 Long itemId = 1L;
		 Integer hits = webSearchResultsPeer.getHits(itemId);
		 if (hits != null)
			 System.out.println("Got "+hits+" for item "+itemId);
		 else
			 System.out.println("Got no hits for item "+itemId);
	 }
	 
	 @Test
	 public void testGetLikeHitsForUser()
	 {
		 List<DBpediaItemSearch> r = webSearchResultsPeer.getHitsInRange(2724L, "facebook", 2000, 1);
		 for(DBpediaItemSearch i : r)
			 System.out.println("user "+i.getUserId()+"like:"+i.getName()+" hits "+i.getHits());
	 }
	 
	 @Test
	 public void testGetHitsInRange()
	 {
		 List<DBpediaItemSearch> r = webSearchResultsPeer.getHitsInRange(2000, 1);
		 for(DBpediaItemSearch i : r)
			 System.out.println("like:"+i.getName()+" hits "+i.getHits());
	 }
	 
	 @Test
	 public void testGetHitsInRangeCheckpoint()
	 {
		 List<DBpediaItemSearch> r = webSearchResultsPeer.getHitsInRange(2000, 100000, 1);
		 for(DBpediaItemSearch i : r)
			 System.out.println("like:"+i.getName()+" hits "+i.getHits());
	 }
	 
	 @Test
	 public void testStoreItem()
	 {
		 Collection<Item> items = itemPeer.getItems(0,10);
		 List<ItemBean> itemBeans = new ArrayList<>();
		 for(Item item : items)
			 itemBeans.add(new ItemBean(item));
		 webSearchResultsPeer.storeNewItems(itemBeans);
	 }

}
