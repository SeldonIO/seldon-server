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

package io.seldon.test.clustering.recommender.jdo;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import junit.framework.Assert;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.clustering.recommender.jdo.AsyncClusterCountStore;
import io.seldon.clustering.recommender.jdo.AsyncClusterCountStore.ClusterCount;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;

public class AsyncClusterCountLoadTest extends BasePeerTest {

	@Autowired PersistenceManager pm;
	
	@Autowired
	GenericPropertyHolder props;
	
	private void clearClusterCounts()
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "delete from  cluster_counts");
					query.execute();
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
		
	}
	
	private Long getNumberCounts()
	{
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select count(*) from cluster_counts");
		query.setResultClass(Long.class);
		query.setUnique(true);
		Long res = (Long) query.execute();
		query.closeAll();
		return res;
	}
	
	@Test 
	public void testLoad() throws InterruptedException
	{
		int numThreads = 40;
		List<Thread> threads = new ArrayList<Thread>();
	
		
		final AsyncClusterCountStore asyncStore = new AsyncClusterCountStore(props.getClient(), 3, 10000, Integer.MAX_VALUE, 3, 22400,true);
		
		Thread queueRunner = new Thread(asyncStore);
		queueRunner.start();
		
		final long numInserts = 10000;
		
		for(int i=0;i<numThreads;i++)
		{
			Runnable r = new Runnable() { public void run()
			{
				Random rand = new Random();
				for(int i=0;i<numInserts;i++)
				{
					ClusterCount cc = new ClusterCount(rand.nextInt(40),rand.nextInt(100),1,1);
					asyncStore.put(cc);
				}
			}
			};
			Thread t = new Thread(r);
			threads.add(t);
			t.start();
		}
		
		for(Thread t : threads)
			t.join();
		
		int qSize;
		while((qSize = asyncStore.getQSize()) > 0)
		{
			System.out.println("Sleeping... as q size is "+qSize);
			Thread.sleep(4000);
		}

		Long numCounts = getNumberCounts();
		System.out.println("Number of counts is "+numCounts);
		Assert.assertNotNull(numCounts);

		//cleanup
		//clearClusterCounts();

	}

}
