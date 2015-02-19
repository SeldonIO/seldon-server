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

package io.seldon.test.cooc.jdbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.cooc.CooccurrenceCount;
import io.seldon.cooc.CooccurrencePeer;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class JDBCCooccurrenceStoreTest  extends BasePeerTest {

	@Autowired
	GenericPropertyHolder props;
	
	private void removeCounts()
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) {
				public void process() {
					Query query = pm.newQuery("javax.jdo.query.SQL", "delete from cooc_counts;");
					query.execute();
				}
			});
		} catch (DatabaseException e)
		{
			logger.error("Failed to remove counts");
		}
		
	}
	
	
	private void insertCount(final long item1,final long item2,final double count,final long time)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL","insert into cooc_counts (item_id1,item_id2,count,time) values (?,?,?,?)");
			    	List<Object> args = new ArrayList<>();
			    	args.add(item1);
			    	args.add(item2);
			    	args.add(count);
			    	args.add(time);
					query.executeWithArray(args.toArray());
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to add count ");
		}
		
	}
	
	@Test
	public void testGetCounts()
	{
		try
		{
			Map<String,CooccurrenceCount> map = new HashMap<>();
			Random r = new Random();
			for(int i1 = 0;i1<5;i1++)
				for(int i2=0;i2<5;i2++)
				{
					if (i1 <= i2)
					{
						double count = r.nextDouble()*100;
						long time = 0;
						insertCount(i1,i2,count,time);
						map.put(CooccurrencePeer.getKey(i1, i2), new CooccurrenceCount(count,time));
					}
				}
			
			List<Long> item11 = new ArrayList<>();
			for(int i=0;i<5;i++)
				item11.add((long)i);
			
			Map<String,CooccurrenceCount> counts = coocStore.getCounts(item11);
			
			Assert.assertEquals(item11.size(),counts.size());
			for(Map.Entry<String, CooccurrenceCount> e : counts.entrySet())
			{
				CooccurrenceCount c1 = e.getValue();
				CooccurrenceCount c2 = map.get(e.getKey());
				Assert.assertEquals(c1.getCount(), c2.getCount(),0.001);
				Assert.assertEquals(c1.getTime(), c2.getTime());
			}
			
			counts = coocStore.getCounts(item11,item11);
			
			Assert.assertEquals(map.size(),counts.size());
			for(Map.Entry<String, CooccurrenceCount> e : counts.entrySet())
			{
				CooccurrenceCount c1 = e.getValue();
				CooccurrenceCount c2 = map.get(e.getKey());
				Assert.assertEquals(c1.getCount(), c2.getCount(),0.001);
				Assert.assertEquals(c1.getTime(), c2.getTime());
			}
		}
		finally
		{
			removeCounts();
		}
	}
	
}
